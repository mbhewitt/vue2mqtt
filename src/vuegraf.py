#!/usr/bin/env python3

import datetime
import json
import signal
import sys
import time
from threading import Event
from collections import defaultdict

# InfluxDB v1
import influxdb

# InfluxDB v2
import influxdb_client

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

influxVersion = 1
if 'version' in config['influxDb']:
    influxVersion = config['influxDb']['version']

bucket = ''
write_api = None
query_api = None
if influxVersion == 2:
    info('Using InfluxDB version 2')
    bucket = config['influxDb']['bucket']
    org = config['influxDb']['org']
    token = config['influxDb']['token']
    url= config['influxDb']['url']
    influx2 = influxdb_client.InfluxDBClient(
       url=url,
       token=token,
       org=org
    )
    write_api = influx2.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)
    query_api = influx2.query_api()

    if config['influxDb']['reset']:
        info('Resetting database')
        delete_api = influx2.delete_api()
        start = "1970-01-01T00:00:00Z"
        stop = datetime.datetime.utcnow().isoformat(timespec='seconds')
        delete_api.delete(start, stop, '_measurement="energy_usage"', bucket=bucket, org=org)    
else:
    info('Using InfluxDB version 1')
    # Only authenticate to ingress if 'user' entry was provided in config
    if 'user' in config['influxDb']:
        influx = influxdb.InfluxDBClient(host=config['influxDb']['host'], port=config['influxDb']['port'], username=config['influxDb']['user'], password=config['influxDb']['pass'], database=config['influxDb']['database'])
    else:
        influx = influxdb.InfluxDBClient(host=config['influxDb']['host'], port=config['influxDb']['port'], database=config['influxDb']['database'])

    influx.create_database(config['influxDb']['database'])

    if config['influxDb']['reset']:
        info('Resetting database')
        influx.delete_series(measurement='energy_usage')

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

INTERVAL_SECS=config["options"]["initial_interval_secs"]

LAG_SECS=5
chan_names=set()
mark=znow=timestamp=old_timestamp=datetime.datetime.now()
output_usage_hr_previous=defaultdict(lambda: 0)
while running:
    for account in config["accounts"]:
        tmpEndingTime = datetime.datetime.utcnow() - datetime.timedelta(seconds=LAG_SECS)

        if 'vue' not in account:
            account['vue'] = PyEmVue()
            account['vue'].login(username=account['email'], password=account['password'])
            info('Login completed')

            populateDevices(account)

            account['end'] = tmpEndingTime

            start = account['end'] - datetime.timedelta(seconds=INTERVAL_SECS)

            tmpStartingTime = start
            timeStr = ''
            if influxVersion == 2:
                timeCol = '_time'
                result = query_api.query('from(bucket:"' + bucket + '") ' +
                                         '|> range(start: -3w) ' +
                                         '|> filter(fn: (r) => ' +
                                         '  r._measurement == "energy_usage" and ' +
                                         '  r._field == "usage" and ' +
                                         '  r.account_name == "' + account['name'] + '")' +
                                         '|> last()')

                if len(result) > 0 and len(result[0].records) > 0:
                    lastRecord = result[0].records[0]
                    timeStr = lastRecord['_time'].isoformat()
            else:
                result = influx.query('select last(usage), time from energy_usage where account_name = \'{}\''.format(account['name']))
                if len(result) > 0:
                    timeStr = next(result.get_points())['time']

            if len(timeStr) > 0:
                timeStr = timeStr[:26]
                if not timeStr.endswith('Z'):
                    timeStr = timeStr + 'Z'

                try:
                    tmpStartingTime = datetime.datetime.strptime(timeStr, '%Y-%m-%dT%H:%M:%S.%fZ')
                except:
                    tmpStartingTime = datetime.datetime.strptime(timeStr, '%Y-%m-%dT%H:%M:%SZ')

            # Avoid overlapping data in the event that vuegraf is quickly restarted.
            # This does not attempt to fill in gaps for scenarios when vuegraf is offline for long
            # periods.
#            if tmpStartingTime > start:
            start = tmpStartingTime
            info("Continuing from most recent record at time {}".format(start))
#            else:
#                info("Starting as of now {} (last known time is {})".format(start, tmpStartingTime))
        else:
            start = account['end'].replace(microsecond=0) + datetime.timedelta(seconds=1)
            account['end'] = tmpEndingTime.replace(microsecond=0)
#            info(f"{start} to {tmpEndingTime}")

        try:
            deviceGids = list(account['deviceIdMap'].keys())
#            channels = account['vue'].get_devices_usage(deviceGids, None, scale=Scale.DAY.value, unit=Unit.KWH.value)
            usageDataPoints = []
            device = None
            secondsInAnHour = 3600
            wattsInAKw = 1000
            channels = account['vue'].get_devices_usage(deviceGids,None)
            ts_diff=0
            output_usage_hr=defaultdict(lambda: 0)
            output_usage_hr_diff=defaultdict(lambda: 0)
            output_usage_id=defaultdict(lambda: 0)
            active_chan_count=0
            for chan in channels:
                chanName = lookupChannelName(account, chan)
                usage=chan.usage*secondsInAnHour*wattsInAKw
                timestamp = datetime.datetime.utcfromtimestamp(chan.timestamp)
                ts_diff=(timestamp-old_timestamp).total_seconds()
                if(ts_diff<config["options"]["nominal_update_rate"]):
                    INTERVAL_SECS+=config["options"]["under_interval_to_add"]
                    break
                unique_id=f"{chan.device_gid}-{chan.channel_num}"
                hrChanName=chanName if(config["options"]["hr_same_name_circuit_join"]) else f"{chanName}-{unique_id}"
                chan_names.add(hrChanName)
                if(usage>config["options"]["min_value_to_ignore"]):
                    active_chan_count+=1
                    output_usage_id[unique_id]=usage
                    output_usage_hr[hrChanName]+=usage
            if(active_chan_count>0):
                if(len(output_usage_hr_previous)>0):
                    for id in chan_names:
                        if(abs(output_usage_hr_previous[id]-output_usage_hr[id])>=config["options"]["min_diff_watts"]):
                            output_usage_hr_diff[id]=output_usage_hr[id]-output_usage_hr_previous[id]
                        if(abs(output_usage_hr_previous[id]-output_usage_hr[id])>0):
                            info(f"{id}\t{output_usage_hr[id]:.0f}\t{output_usage_hr_diff[id]:.0f}\t{timestamp}")
                output_usage_hr_previous=output_usage_hr.copy()
            znow=datetime.datetime.utcnow()

            diff=(znow-mark).total_seconds()
            real_diff=(znow-timestamp).total_seconds()
            if(real_diff>config["options"]["max_target_lag"]):
                INTERVAL_SECS-=config["options"]["over_lag_interval_to_sub"]
            if(ts_diff>config["options"]["nominal_update_rate"]):
                INTERVAL_SECS-=config["options"]["over_interval_to_sub"]
            if(ts_diff>0):
                mark=znow
            INTERVAL_SECS=max(config["options"]["min_interval"],min(config["options"]["max_interval"],INTERVAL_SECS))
            info(f"{real_diff} {diff} {INTERVAL_SECS} {ts_diff}")
            old_timestamp=timestamp
            pauseEvent.wait(INTERVAL_SECS)
            continue
            for chan in channels:
                chanName = lookupChannelName(account, chan)

                usage, usage_start_time = account['vue'].get_chart_usage(chan, start, account['end'], scale=Scale.SECOND.value, unit=Unit.KWH.value)
                len_usage=len(usage)
                info(f"{chanName} {usage_start_time} {len_usage}")
                index = 0
                for kwhUsage in usage:
                    if kwhUsage is not None:
                        watts = float(secondsInAnHour * wattsInAKw) * kwhUsage
                        if influxVersion == 2:
                            dataPoint = influxdb_client.Point("energy_usage").tag("account_name", account['name']).tag("device_name", chanName).field("usage", watts).time(time=start + datetime.timedelta(seconds=index))
                            usageDataPoints.append(dataPoint)
                        else:
                            dataPoint = {
                                "measurement": "energy_usage",
                                "tags": {
                                    "account_name": account['name'],
                                    "device_name": chanName,
                                },
                                "fields": {
                                    "usage": watts,
                                },
                                "time": start + datetime.timedelta(seconds=index)
                            }
                            usageDataPoints.append(dataPoint)
                        index = index + 1

            info('Submitted datapoints to database; account="{}"; points={}'.format(account['name'], len(usageDataPoints)))
            if influxVersion == 2:
                write_api.write(bucket=bucket, record=usageDataPoints)
            else:
                influx.write_points(usageDataPoints)
        except:
            error('Failed to record new usage data: {}'.format(sys.exc_info())) 

#    pauseEvent.wait(INTERVAL_SECS)

info('Finished')

