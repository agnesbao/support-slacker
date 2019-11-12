import argparse
import json
import os
from datetime import datetime

import pandas as pd
import slacker


class SupportTracker:
    """A class that helps track support requests"""
    MAX_PAGE_SIZE = 100
    TOKEN_ENV_VAR = "SLACK_TOKEN"

    def __init__(self):
        # Set up Slack client
        self.client = slacker.Slacker(token=os.environ[self.TOKEN_ENV_VAR])
        self.client.auth.test()

    def get_messages(self, handle: str) -> list:
        """Get info about messages where users pinged a support handle

        :param handle: The support handle (don't include '@')

        :return: A pd.DataFrame of information about each matching message.
            Has asker_handle, timestamp, msg_text, and slack_link columns.
        """
        # The query is just the ID of the support handle
        query = self._get_usergroup_id(handle)

        this_page, last_page, messages = self._get_some_messages(
            query=query,
            page_num=1
        )

        # Scoll over messages past the first page
        while this_page != last_page:
            this_page, last_page, new_messages = self._get_some_messages(
                query=query,
                page_num=this_page + 1
            )
            messages += new_messages

        return pd.DataFrame(messages)
    
    def get_users_df(self) -> list:
        users_list = self.client.users.list().body['members']
        users = [
            {'id': user['id'], 
             'handle': user['name'],
             'real_name': user['profile']['real_name_normalized'],
             'is_bot': user['is_bot']
             } 
            for user in users_list
        ]
        return pd.DataFrame(users)
    
    def get_thread(self, url: str) -> list:
        thread_ts = self._extract_thread_ts(url)
        if thread_ts:
            channel = self._extract_channel(url)
            res = self.client.channels.replies(channel, thread_ts)        
            channel_msg = res.body['messages'][0]
            last_msg_ts = datetime.fromtimestamp(int(float(channel_msg['latest_reply'])))
            asker_id = channel_msg['user']
            ask_ts = datetime.fromtimestamp(int(float(channel_msg['ts'])))
            responder_id, respond_ts = self._get_respond(channel_msg['replies'], asker_id)
            return [asker_id, ask_ts, responder_id, respond_ts, last_msg_ts]
        else:
            return [None]*5
    
    def _extract_channel(self, url: str) -> str:
        return url.split("/")[4]
    
    def _extract_thread_ts(self, url: str) -> str:
        try:
            return url.split("?thread_ts=")[1]
        except IndexError:
            return None

    def _get_some_messages(self, query: str, page_num: int) -> list:
        """Get and parse one page of messages matching a query"""
        res = self.client.search.messages(
            query=query,
            sort='timestamp',
            sort_dir='asc',
            count=self.MAX_PAGE_SIZE,
            page=page_num
        )

        this_page = res.body['messages']['pagination']['page']
        last_page = res.body['messages']['pagination']['page_count']
        messages = [
            {
                'mentioner_id': msg['user'],
                'mentioner_handle': msg['username'],
                'timestamp': datetime.fromtimestamp(int(float(msg['ts']))),
                'msg_text': msg['text'],
                'slack_link': msg['permalink']
            }
            for msg in res.body['messages']['matches']
            if msg['username'] != 'slackbot'
        ]

        return this_page, last_page, messages
    
    def _get_respond(self, replies: list, asker_id: str) -> list:
        for reply in replies:
            if reply['user']!=asker_id:
                return reply['user'], datetime.fromtimestamp(int(float(reply['ts'])))
        return None, None

    def _get_usergroup_id(self, handle: str) -> str:
        """Get the ID of the support handle"""
        return [
            usergroup['id']
            for usergroup in self.client.usergroups.list().body['usergroups']
            if usergroup['handle'] == handle
        ][0]
        
def get_name(df, id_col, users_df):
    df = df.merge(users_df[['id','real_name']], how='left', left_on=id_col, right_on='id').rename(columns={'real_name': id_col.replace("id","name")})
    df = df.drop(axis=1, columns=[id_col, 'id'])
    return df

def run_slack_tracker(support_handle, min_date=None, max_date=None, output_path='./support_requests.csv'):
    st = SupportTracker()
    print("Getting messages with support handle...")
    message_df = st.get_messages(support_handle)
    if min_date:
        message_df = message_df[message_df['timestamp']>=pd.to_datetime(min_date)]
    if max_date:
        message_df = message_df[message_df['timestamp']<=pd.to_datetime(max_date)]
    print("Getting thread of messages...")
    message_df = message_df.sort_values(by='timestamp')
    message_df['thread_ts'] = message_df['slack_link'].apply(st._extract_thread_ts)
    message_df = message_df[(~message_df['thread_ts'].duplicated()) | (message_df['thread_ts'].isnull())]
    message_df = message_df.dropna(subset=['mentioner_id']) # this will drop workflow bot
    thread_res = message_df['slack_link'].apply(st.get_thread)
    message_df[['asker_id','ask_ts','responder_id','respond_ts','last_msg_ts']] = pd.DataFrame(thread_res.to_list(),index=thread_res.index)
    
    users_df = st.get_users_df()
    message_df = get_name(message_df, "mentioner_id", users_df)
    message_df = get_name(message_df, "asker_id", users_df)
    message_df = get_name(message_df, "responder_id", users_df)

    print("Saving csv to", output_path)
    message_df[sorted(message_df.columns)].to_csv(output_path, index=False)
    


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Save support request information to CSV. Your Slack token '
            'should be saved in the SLACK_TOKEN environment variable. See how '
            'to get one at https://api.slack.com/custom-integrations/legacy-tokens',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--support_handle',
        help="The support handle (don't include '@')"
    )
    parser.add_argument(
        '--output_path',
        help='Path to save a CSV of support request information',
        default='./support_requests.csv'
    )
    parser.add_argument(
        '--min_date',
        help='Earliest date to return message, in format yyyy-mm-dd',
        default=None
    )
    parser.add_argument(
        '--max_date',
        help='Latest date to return message, in format yyyy-mm-dd',
        default=None
    )
    args = parser.parse_args()

    assert args.support_handle, 'Must provide the --support_handle arg'
    
    run_slack_tracker(args.support_handle, args.min_date, args.max_date, args.output_path)
