#!/usr/bin/env python3

import datetime
import json
import signal
import sys
import time
from threading import Event
from collections import defaultdict
import paho.mqtt.client as mqtt

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

def handleExit(signum, frame):
    global running
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

def lookupChannelName(account, chan):
    if chan.device_gid not in account['deviceIdMap']:
        populateDevices(account)

    deviceName = lookupDeviceName(account, chan.device_gid)
    name = "{}-{}".format(deviceName, chan.channel_num)
    if 'devices' in account:
        for device in account['devices']:
            if 'name' in device and device['name'] == deviceName:
                try:
                    num = int(chan.channel_num)
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

def embelish_tags(tags,id,account,chan,unique_id,hrChanName,chanName,deviceName):
    tags[id]['account']=account["name"]
    tags[id]['device']=deviceName
    tags[id]['chanName']=chanName
    tags[id]['unique_id']=unique_id
    tags[id]['device_gid']=chan.device_gid
    tags[id]['channel_num']=chan.channel_num
    tags[id]['name']=hrChanName
    tags[id]['id']=id

def runChannels(account,channels,old_timestamp,output,chan_names,tags):
    active_chan_count=0
    timestamp=old_timestamp
    chan_timestamp=0
    for chan in channels:
        deviceName = lookupDeviceName(account, chan.device_gid)
        chanName = lookupChannelName(account, chan)
        usage=chan.usage*secondsInAnHour*wattsInAKw
        timestamp = datetime.datetime.utcfromtimestamp(chan.timestamp)
        chan_timestamp = chan.timestamp
        ts_diff=(timestamp-old_timestamp).total_seconds()
        if(ts_diff<config["options"]["nominal_update_rate"]):
            break
        unique_id=f"{chan.device_gid}-{chan.channel_num}"
        hrChanName=chanName if(config["options"]["hr_same_name_circuit_join"]) else f"{chanName}-{unique_id}"
        chan_names.add(hrChanName)
        embelish_tags(tags,unique_id,account,chan,unique_id,hrChanName,chanName,deviceName)
        embelish_tags(tags,hrChanName,account,chan,unique_id,hrChanName,chanName,deviceName)
        if(usage>config["options"]["min_value_to_ignore"]):
            active_chan_count+=1
            output['usage_id'][unique_id]=usage
            output['usage_hr'][hrChanName]+=usage
    return (active_chan_count,timestamp,chan_timestamp)

def modify_interval(account,timestamp,old_timestamp):
    znow=datetime.datetime.utcnow()

    real_diff=(znow-timestamp).total_seconds()

    ts_diff=(timestamp-old_timestamp).total_seconds()
    if(ts_diff<config["options"]["nominal_update_rate"]):
        account['INTERVAL_SECS']+=config["options"]["under_interval_to_add"]
    if(real_diff>config["options"]["max_target_lag"]):
        account['INTERVAL_SECS']-=config["options"]["over_lag_interval_to_sub"]
    if(ts_diff>config["options"]["nominal_update_rate"]):
        account['INTERVAL_SECS']-=config["options"]["over_interval_to_sub"]

    account['INTERVAL_SECS']=max(config["options"]["min_interval"],min(config["options"]["max_interval"],account['INTERVAL_SECS']))

#    info(f"{real_diff} {account['INTERVAL_SECS']} {ts_diff}")

def mqtt_connect():
    client = mqtt.Client("emporia_mqtt",False)
    client.connect(config['mqtt']['host'],config['mqtt']['port'])
    client.max_inflight_messages_set(10000)
    return client

def find_diff(output,active_chan_count,chan_names):
    if(active_chan_count>0):
        if(len(output['usage_hr_previous'])>0):
            for id in chan_names:
                diff=output['usage_hr'][id]-output['usage_hr_previous'][id]
                perc=abs(diff)/output['usage_hr'][id] if output['usage_hr'][id] != 0 else 1
                if(abs(diff)>=config["options"]["min_diff_watts"] and perc>config["options"]["min_diff_watts_perc"]):
                    output['usage_hr_diff'][id]=diff
#                if(abs(output['usage_hr_diff'][id]-output['usage_hr'][id])>0 ):
#                    info(f"{id}\t{output['usage_hr'][id]:.0f}\t{output['usage_hr_diff'][id]:.0f} {perc:.4f}")
        output['usage_hr_previous']=output['usage_hr'].copy()

def format_output_mqtt(output,tags,timestamp,client):
    out=[]
    client.reconnect()
    for type in config['mqtt']['output'].keys():
        if(config['mqtt']['output'][type]['output_format']=="influx" and config['mqtt']['output'][type]['enable']):
            for id,v in output[type].items():
                if(abs(v)<=config["options"]["min_value_to_ignore"]):
                    continue
                metric = Metric(config['mqtt']['output'][type]['measurement'])
                metric.with_timestamp(timestamp*1000000000)
                metric.add_value('usage',v)
                for m in config['mqtt']['output'][type]['tags']:
                    metric.add_tag(m,tags[id][m])
                topic=config['mqtt']['output'][type]['pattern'].format(**tags[id])
                rc=client.publish(topic,f"{metric}",1)
                info(f"{topic} {metric}")
                out.append((topic,metric))
    info(f"{len(out)} Metrics sent to mqtt")
    return out

chan_names=set()
old_timestamp=datetime.datetime.utcnow()
output={}
output['usage_hr_previous']=defaultdict(lambda: 0)
client=mqtt_connect()
while running:
    for account in config["accounts"]:
        logInAndInit(account)

        try:
            deviceGids = list(account['deviceIdMap'].keys())
            channels = account['vue'].get_devices_usage(deviceGids,None)
            output['usage_hr']=defaultdict(lambda: 0)
            output['usage_hr_diff']=defaultdict(lambda: 0)
            output['usage_id']=defaultdict(lambda: 0)
            tags=defaultdict(lambda: {})
            (active_chan_count,timestamp,chan_timestamp)=runChannels(account,channels,old_timestamp,output,chan_names,tags)
            modify_interval(account,timestamp,old_timestamp)
            find_diff(output,active_chan_count,chan_names)
            out_values=format_output_mqtt(output,tags,chan_timestamp,client)
            print("")
            old_timestamp=timestamp
            pauseEvent.wait(account['INTERVAL_SECS'])
        except:
            error('Failed to record new usage data: {}'.format(sys.exc_info())) 

info('Finished')

