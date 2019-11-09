#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 15:13:09 2019

@author: abao
"""

from datetime import datetime

import pandas as pd
import track_support_requests as tsr

def get_date(ts):
    try:
        return ts.strftime("%m/%d/%Y")
    except ValueError:
        return None

def get_time(ts):
    try:
        return ts.strftime("%I:%M:%S %p")
    except ValueError:
        return None
    
# change min_date before running
min_date = datetime(2019,11,4)
tsr.run_slack_tracker("uptakeio-support", min_date=min_date)

message_df = pd.read_csv("support_requests.csv", parse_dates=['ask_ts', 'respond_ts', 'last_msg_ts'])
message_df['date_asked'] = message_df['ask_ts'].apply(get_date)
message_df['time_asked'] = message_df['ask_ts'].apply(get_time)
message_df['date_responded'] = message_df['respond_ts'].apply(get_date)
message_df['time_responded'] = message_df['respond_ts'].apply(get_time)
message_df['date_resolved'] = message_df['last_msg_ts'].apply(get_date)
message_df['time_resolved'] = message_df['last_msg_ts'].apply(get_time)


message_df['month_asked'] = None
message_df['resolution'] = None

message_df[['date_asked','month_asked','time_asked',
            'asker_name','date_responded','time_responded',
            'responder_name','date_resolved','time_resolved',
            'msg_text','resolution','slack_link',]].to_csv("final_support_requests.csv", index=False)