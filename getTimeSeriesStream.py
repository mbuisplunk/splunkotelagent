#!/usr/bin/env python
#
# This script will return a dataframe that contains streamed timeseries data with corresponding metadata
#
# Edit the required configuration section before script execution
#
# Syntax: python3 getTimeseriesStream.py

import asyncio
import signalfx
import pandas as pd
pd.set_option('display.max_rows', 500)
from pandas.io.json import json_normalize
import datetime
import time
import json
import warnings
from IPython.display import display
warnings.simplefilter(action='ignore', category=FutureWarning)

## required configuration
token=""
org = ""
resolution = 60 * 60 * 1000
startDate=1667484000000     # Start date (milliseconds) -- starting streaming window
stopDate=1667595600000      # End date (milliseconds) -- ending streaming window
program = "data('cpu.utilization').publish()"       ## program and filtering needs to be defined
# example: program = "data('cpu.utilization', filter=filter('env', 'prod') and filter('aws_state', '{Code: 16,Name: running}')).publish()"

## initialized variables
finaldict = {}
datacontainer ={}
sfx = signalfx.SignalFx(stream_endpoint='https://stream.{0}.signalfx.com'.format(org))

## function to stream timeseries based on startDate and stopDate window
with sfx.signalflow(token) as flow:
    computation = flow.execute(program, start=startDate, stop=stopDate, resolution=resolution)
    df = pd.DataFrame([])
    data = {}
    msgdict = {}
    dfmsg = {}

    ## timestamp and value is added to a dataframe
    for msg in computation.stream():
        if isinstance(msg, signalfx.signalflow.messages.DataMessage):
            y = {"timestamp": msg.logical_timestamp_ms}
            x = {"data": msg.data}
            dfmsg.update(y)
            dfmsg.update(x)
            df = df.append(dfmsg, ignore_index=True)

    ## dataframe iterated for metadata to be appended
    for index, row in df.iterrows():
        for i in row['data']:
            metadata = computation.get_metadata(i)
            z = {"metadata": metadata}
            dfmsg.update(z)
            df = df.append(dfmsg, ignore_index=True)

## resulting dataframe is displayed in following format: timestamp || data || metadata
display(df)
#df.to_csv('output.csv')