import sys
import os
import pandas as pd
import datetime
from poloniex import Poloniex
from boto3.session import Session
from io import StringIO


def save_to_s3(df, aws_key, aws_secret, bucket_name, key):
    session = Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    s3 = session.resource('s3')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    s3.Object(bucket_name, key).put(Body=csv_buffer.getvalue())


def get_poloniex_history(key, secret, start_ts, end_ts):
    polo = Poloniex(key, secret)

    trade_history = polo.returnTradeHistory(currencyPair='all', start=start_ts, end=end_ts)

    result = None
    for key in trade_history.keys():
        trades = pd.DataFrame.from_dict(trade_history[key])
        trades['currency'] = key
        result = trades if result is None else result.append(trades)

    return result


def poloniex_trade_handler(event, context):
    polo_key = event['POLONIEX_KEY']
    polo_secret = event['POLONIEX_SECRET']
    start = os.environ['START_TIME']
    end = os.environ['END_TIME']
    aws_key = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']
    bucket_name = os.environ['AWS_S3_BUCKET']
    key = os.environ['AWS_S3_KEY']

    history = get_poloniex_history(polo_key, polo_secret, start, end)
    save_to_s3(history, aws_key, aws_secret, bucket_name, key)


def main():
    dt = int(datetime.datetime.now().timestamp())
    os.environ['AWS_S3_KEY'] = str(dt) + '_poloniex_trades.csv'

    event = {'POLONIEX_KEY': os.environ['POLONIEX_KEY'], 'POLONIEX_SECRET': os.environ['POLONIEX_SECRET']}

    poloniex_trade_handler(event, None)


if __name__ == '__main__':
    sys.exit(main())