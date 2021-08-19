#!/usr/bin/env python3

import datetime
import json
import signal
import sys
import time
from threading import Event
from collections import defaultdict
import paho.mqtt.client as mqtt
import traceback

from influx_line_protocol import Metric

from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit

# flush=True helps when running in a container without a tty attached
# (alternatively, "python -u" or PYTHONUNBUFFERED will help here)
def log(level, msg):
    now = datetime.datetime.utcnow()
    print('{} | {} | {}'.format(now, level.ljust(5), msg), flush=True)

def info(msg):
    log("INFO", msg)

def error(msg):
    log("ERROR", msg)

client=None
failed_instant_list=[]


def handleExit(signum, frame):
    global running
    client.loop_stop()
    error('Caught exit signal')
    running = False
    pauseEvent.set()

if len(sys.argv) != 2:
    print('Usage: python {} <config-file>'.format(sys.argv[0]))
    sys.exit(1)

configFilename = sys.argv[1]
config = {}
with open(configFilename) as configFile:
    config = json.load(configFile)

running = True

def populateDevices(account):
    deviceIdMap = {}
    account['deviceIdMap'] = deviceIdMap
    channelIdMap = {}
    account['channelIdMap'] = channelIdMap
    devices = account['vue'].get_devices()
    for device in devices:
        device = account['vue'].populate_device_properties(device)
        deviceIdMap[device.device_gid] = device
        for chan in device.channels:
            key = "{}-{}".format(device.device_gid, chan.channel_num)
            if chan.name is None and chan.channel_num == '1,2,3':
                chan.name = device.device_name
            channelIdMap[key] = chan
            info("Discovered new channel: {} ({})".format(chan.name, chan.channel_num))

def lookupDeviceName(account, device_gid):
    if device_gid not in account['deviceIdMap']:
        populateDevices(account)

    deviceName = "{}".format(device_gid)
    if device_gid in account['deviceIdMap']:
        deviceName = account['deviceIdMap'][device_gid].device_name
    return deviceName

def lookupChannelName(account, chan,device_gid,channel_num):
    if device_gid not in account['deviceIdMap']:
        populateDevices(account)

    deviceName = lookupDeviceName(account, device_gid)
    name = "{}-{}".format(deviceName, channel_num)
    if 'devices' in account:
        for device in account['devices']:
            if 'name' in device and device['name'] == deviceName:
                try:
                    num = int(channel_num)
                    if 'channels' in device and len(device['channels']) >= num:
                        name = device['channels'][num - 1]
                except:
                    name = deviceName
    return name

signal.signal(signal.SIGINT, handleExit)
signal.signal(signal.SIGHUP, handleExit)

pauseEvent = Event()
secondsInAnHour = 3600
wattsInAKw = 1000



def logInAndInit(account):
    if 'vue' not in account:
        account['vue'] = PyEmVue()
        account['vue'].login(username=account['email'], password=account['password'])
        info('Login completed')

        account['INTERVAL_SECS']=config["options"]["initial_interval_secs"]
        populateDevices(account)

def embelish_tags(tags,id,account,chan,unique_id,hrChanName,chanName,deviceName,device_gid,channel_num):
    tags[id]['account']=account["name"]
    tags[id]['device']=deviceName
    tags[id]['chanName']=chanName
    tags[id]['unique_id']=unique_id
    tags[id]['device_gid']=device_gid
    tags[id]['channel_num']=channel_num
    tags[id]['branchName']=hrChanName
    tags[id]['id']=id

def runChannels(account,channels,output,chan_names,tags,out,client,device_gid):
    active_chan_count=0
    chan_timestamp=0
    for (channel_num,chan) in channels.items():
        if(chan.usage == None):
            continue
        deviceName = lookupDeviceName(account, device_gid)
        chanName = lookupChannelName(account, chan,device_gid,channel_num)
        usage=chan.usage*secondsInAnHour*wattsInAKw
        chan_timestamp = chan.timestamp.timestamp()

        unique_id=f"{device_gid}-{channel_num}"
        hrChanName=chanName if(config["options"]["hr_same_name_circuit_join"]) else f"{chanName}-{unique_id}"
        chan_names.add(hrChanName)
        embelish_tags(tags,unique_id,account,chan,unique_id,hrChanName,chanName,deviceName,device_gid,channel_num)
        embelish_tags(tags,hrChanName,account,chan,unique_id,hrChanName,chanName,deviceName,device_gid,channel_num)
        if(usage>config["options"]["min_value_to_ignore"]):
            active_chan_count+=1
            send_mqtt(tags,chan_timestamp,client,'usage_id',unique_id,usage,out)
            output['usage_id'][unique_id]=usage
            output['usage_hr'][hrChanName]+=usage
    if(active_chan_count>0):
        for hrChanName in chan_names:
            usage=output['usage_hr'][hrChanName]
            if(output['usage_hr_previous'][hrChanName]!=usage or int(time.time())%30==0):
                send_mqtt(tags,chan_timestamp,client,'usage_hr',hrChanName,usage,out)
    return active_chan_count

def modify_interval(account,timestamp,old_timestamp,znow):

    real_diff=(znow-timestamp).total_seconds()

    ts_diff=(timestamp-old_timestamp).total_seconds()
    if(ts_diff<config["options"]["nominal_update_rate"]):
        account['INTERVAL_SECS']+=config["options"]["under_interval_to_add"]
    if(real_diff>config["options"]["max_target_lag"]):
        account['INTERVAL_SECS']-=config["options"]["over_lag_interval_to_sub"]
    if(real_diff>config["options"]["super_max_target_lag"]):
        account['INTERVAL_SECS']-=config["options"]["super_over_lag_interval_to_sub"]
    if(ts_diff>config["options"]["nominal_update_rate"]):
        account['INTERVAL_SECS']-=config["options"]["over_interval_to_sub"]

    if(ts_diff>config["options"]["nominal_update_rate"]):
        error(f"OverNominalRate LAG:{real_diff}, ts_diff:{ts_diff}")
    if(real_diff>config["options"]["super_max_target_lag"]):
        error(f"Overdue LAG:{real_diff}")

    account['INTERVAL_SECS']=round(max(config["options"]["min_interval"],min(config["options"]["max_interval"],account['INTERVAL_SECS'])),2)
    return (real_diff,ts_diff)


def classifier(output,tags,out,client,chan_timestamp):
    for (id,v) in output['usage_hr_diff'].items():
        on=config['device_on'].get(id,None)
        if(on == None):
            continue
        for app,item in on.items():
            min=item.get('min',0)
            max=item.get('max',12000)
            abs=item.get('abs',0)
            actual=output['usage_hr'][id]
            if(v>=min and v<=max and actual>abs):
                output["device"][app]=v
                tags[app]=tags[id].copy()
                tags[app]["device"]=app
                tags[app]["onoff"]="on"
                send_mqtt(tags,chan_timestamp,client,'device',app,v,out)
                #info(f"{app} on {v} {actual} {min} {max} {abs}")
            elif(v<=-min and v>=-max or actual<abs and v < -min/3):
                tags[app]=tags[id].copy()
                output["device"][app]=v
                tags[app]["device"]=app
                tags[app]["onoff"]="off"
                send_mqtt(tags,chan_timestamp,client,'device',app,v,out)
                #info(f"{app} off")

def find_diff(output,active_chan_count,chan_names,out_values,client,chan_timestamp,tags):
    if(active_chan_count>0):
        if(len(output['usage_hr_previous'])>0):
            for id in chan_names:
                diff=output['usage_hr'][id]-output['usage_hr_previous'][id]
                perc=abs(diff)/output['usage_hr'][id] if output['usage_hr'][id] != 0 else 1
                if(abs(diff)>=config["options"]["min_diff_watts"] and perc>config["options"]["min_diff_watts_perc"]):
                    output['usage_hr_diff'][id]=diff
                    send_mqtt(tags,chan_timestamp,client,'usage_hr_diff',id,diff,out_values)

#                if(abs(output['usage_hr_diff'][id]-output['usage_hr'][id])>0 ):
#                    info(f"{id}\t{output['usage_hr'][id]:.0f}\t{output['usage_hr_diff'][id]:.0f} {perc:.4f}")
        output['usage_hr_previous']=output['usage_hr'].copy()

def send_mqtt(tags,timestamp,client,type,id,v,out_values):
    if(config['mqtt']['output'][type]['output_format']=="influx" and config['mqtt']['output'][type]['enable']):
        metric = Metric(config['mqtt']['output'][type]['measurement'])
        metric.with_timestamp(timestamp*1000000000)
        metric.add_value('usage',v)
        for m in config['mqtt']['output'][type]['tags']:
            metric.add_tag(m,tags[id][m])
        topic=config['mqtt']['output'][type]['pattern'].format(**tags[id])
        publish_result=client.publish(topic,f"{metric}",1)
        (rc,m)=publish_result
        if(m % 500==0):
           print(f"mqtt message {m}")
        if(rc!=0):
            error(f"publish error {rc} {m}")
            sys.exit(1)
        #info(f"{topic} {metric}")
        out_values.append((topic,metric))

def mqtt_connect():
    client = mqtt.Client("emporia_mqtt",False)
    client.connect(config['mqtt']['host'],config['mqtt']['port'])
    client.max_inflight_messages_set(10000)
    client.max_queued_messages_set(0)
    client.loop_start()
    return client

def InstantRun(account,output,old_timestamp,chan_names,client,success_timestamp,instant,out_values,retry):
    deviceGids = list(account['deviceIdMap'].keys())
    usages = account['vue'].get_device_list_usage(deviceGids,instant)
    output['usage_hr']=defaultdict(lambda: 0.0)
    output['usage_hr_diff']=defaultdict(lambda: 0.0)
    output['usage_id']=defaultdict(lambda: 0.0)
    output['device']=defaultdict(lambda: 0.0)
    tags=defaultdict(lambda: {})

    for gid, device in usages.items():
        channels=device.channels
        chan_timestamp=channels['1,2,3'].timestamp.timestamp()
#        timestamp=chan_timestamp
        timestamp = datetime.datetime.utcfromtimestamp(chan_timestamp)

        active_chan_count=0
        if(chan_timestamp not in success_timestamp):
            active_chan_count=runChannels(account,channels,output,chan_names,tags,out_values,client,gid)
            success_timestamp[chan_timestamp]=instant
        if(active_chan_count>0 and retry==False):
            find_diff(output,active_chan_count,chan_names,out_values,client,chan_timestamp,tags)
            classifier(output,tags,out_values,client,chan_timestamp)

        return (timestamp,active_chan_count)

client=mqtt_connect()

chan_names=set()
output={}
output['usage_hr_previous']=defaultdict(lambda: 0)
loop_now=datetime.datetime.utcnow()
success_timestamp={}
hits=0
old_timestamp=datetime.datetime.utcnow()
failed_instant_list_check=set()

def failed_add(failed_instant_list,failed_instant_list_check,instant):
   instant_sec=instant.replace(microsecond=0)
   if(instant_sec not in failed_instant_list_check):
       failed_instant_list_check.add(instant_sec)
       failed_instant_list.insert(0,instant)

while running:
#def instantRun(hits,instant=None):
    instant=None
    if(instant==None):
        instant = datetime.datetime.utcnow() #- datetime.timedelta(seconds=60)

    for account in config["accounts"]:
        logInAndInit(account)

        try:
            non_loop_time=datetime.datetime.utcnow()

            out_values=[]
            (timestamp,active_chan_count)=InstantRun(account,output,old_timestamp,chan_names,client,success_timestamp,instant,out_values,False)
            hits+=1

            (real_diff,ts_diff)=modify_interval(account,timestamp,old_timestamp,instant)

            loop_diff=(datetime.datetime.utcnow()-loop_now).total_seconds()

            if(loop_diff>config["options"]["nominal_update_rate"]*1.5 or 
                    active_chan_count==0 and loop_diff>config["options"]["nominal_update_rate"]-0.1 or 
                    ts_diff > config["options"]["nominal_update_rate"]):
                failed_add(failed_instant_list,failed_instant_list_check,instant  - datetime.timedelta(seconds=7))
                failed_add(failed_instant_list,failed_instant_list_check,instant  - datetime.timedelta(seconds=5))
                failed_add(failed_instant_list,failed_instant_list_check,instant  - datetime.timedelta(seconds=3))
                #failed_add(failed_instant_list,failed_instant_list_check,instant  - datetime.timedelta(seconds=2))
                failed_add(failed_instant_list,failed_instant_list_check,instant  - datetime.timedelta(seconds=1))
                #failed_add(failed_instant_list,failed_instant_list_check,instant)
                failed_add(failed_instant_list,failed_instant_list_check,instant  + datetime.timedelta(seconds=1))
                #failed_add(failed_instant_list,failed_instant_list_check,instant  + datetime.timedelta(seconds=2))
                failed_add(failed_instant_list,failed_instant_list_check,instant  + datetime.timedelta(seconds=3))
                last_failed_instant=instant

            last_failed_diff=(instant-last_failed_instant).total_seconds()
#            print(last_failed_diff)
            if(last_failed_diff>30 and len(failed_instant_list)>0):
                instantX=failed_instant_list.pop()
                (timestampX,active_chan_countX)=InstantRun(account,output,old_timestamp,chan_names,client,success_timestamp,instantX,out_values,True)
                hits+=1
                info(f"Retry: instant:{instantX}, records:{active_chan_countX}, failed:{len(failed_instant_list):.1f},success:{len(success_timestamp)}")
#            format_output_mqtt(out_values,output,tags,chan_timestamp,client)

            non_loop_time2=datetime.datetime.utcnow()
            non_loop_diff=(non_loop_time2-non_loop_time).total_seconds()
            if(real_diff<config["options"]["best_lag"] and ts_diff == config["options"]["nominal_update_rate"]):
                account['INTERVAL_SECS']=config["options"]["nominal_update_rate"]
            calc_interval=(max(0,account['INTERVAL_SECS']-non_loop_diff))

            if(ts_diff>0 or len(out_values)>0):
                loop_diff=(datetime.datetime.utcnow()-loop_now).total_seconds()
                loop_now=datetime.datetime.utcnow()
                info_int= f"Lag: {round(real_diff,2)}, interval: {account['INTERVAL_SECS']}, ts_diff: {ts_diff}"
                info(f"MQTT sent:{len(out_values)}, {info_int}, server_hits:{hits}, loop_diff:{loop_diff:.1f}, failed:{len(failed_instant_list):.1f},success:{len(success_timestamp)}")
                hits=0
#            print("")
            old_timestamp=timestamp
            pauseEvent.wait(calc_interval)
        except:
            raise
            (e,m,t)=sys.exc_info()
            error('Failed to record new usage data: {},{}\n{}'.format(e,m,traceback.format_tb(t))) 
            try:
                failed_add(failed_instant_list,failed_instant_list_check,instant)
            except:
                (e,m,t)=sys.exc_info()
                error('Failed to record new usage data: {},{}\n{}'.format(e,m,traceback.format_tb(t))) 

#    instantRun(hits,None)

info(f"{failed_instant_list}")
info('Finished')
