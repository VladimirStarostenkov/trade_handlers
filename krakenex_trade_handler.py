import sys
import os
import pandas as pd
import krakenex
import datetime
from boto3.session import Session
from io import StringIO


def save_to_s3(df, aws_key, aws_secret, bucket_name, key):
    session = Session(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    s3 = session.resource('s3')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    s3.Object(bucket_name, key).put(Body=csv_buffer.getvalue())


def get_krakenex_history(key, secret, start_ts, end_ts):
    krak = krakenex.API(key, secret)

    # {'error': [], 'result': {'trades': {}, 'count': 0}}
    # {'trades': {'AAAAAA-BBBBB-CCCCCC': {'ordertxid': 'XXXXXX-YYYYY-ZZZZZZ', 'pair': 'XETHZEUR',
    #                                     'time': 1502554595.7365, 'type': 'buy', 'ordertype': 'market',
    #                                     'price': '268.45000', 'cost': '53.69000', 'fee': '0.13959',
    #                                     'vol': '0.20000000', 'margin': '0.00000', 'misc': ''}}
    trade_history = krak.query_private('TradesHistory', req={'start': start_ts, 'end': end_ts})
    result = pd.DataFrame([dict({'txid': k}, **v) for k, v in trade_history['result']['trades'].items()])

    return result


def krakenex_trade_handler(event, context):
    krak_key = event['KRAKENEX_KEY']
    krak_secret = event['KRAKENEX_SECRET']
    start = os.environ['START_TIME']
    end = os.environ['END_TIME']
    aws_key = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']
    bucket_name = os.environ['AWS_S3_BUCKET']
    key = os.environ['AWS_S3_KEY']

    history = get_krakenex_history(krak_key, krak_secret, start, end)
    save_to_s3(history, aws_key, aws_secret, bucket_name, key)


def main():
    dt = int(datetime.datetime.now().timestamp())
    os.environ['AWS_S3_KEY'] = str(dt) + '_krakenex_trades.csv'

    event = {'KRAKENEX_KEY': os.environ['KRAKENEX_KEY'], 'KRAKENEX_SECRET': os.environ['KRAKENEX_SECRET']}

    krakenex_trade_handler(event, None)


if __name__ == '__main__':
    sys.exit(main())