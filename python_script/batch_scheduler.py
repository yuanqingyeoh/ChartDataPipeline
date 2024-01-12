import datetime
import time
import pycron

from confluent_kafka import Producer

import requests
import json

TOPIC = 'xrp-usdt-batch'

conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': "CRYPTO_XRP_USDT_BATCH_PRODUCER"}


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


@pycron.cron("*/1 * * * *")
async def data_retrieve_job(timestamp: datetime):
    print("Start job at " + str(timestamp))

    temp = int(time.time() * 1000) - (5 * 60 * 1000)

    URL = 'https://api.crypto.com/exchange/v1/public/get-candlestick?instrument_name=XRP_USDT&start_ts=' + str(temp)

    producer = Producer(conf)

    response = requests.get(URL)

    result = response.json()['result']
    # print(json.dumps(result, indent=2))

    print('Sent payload~ :' + json.dumps(result))
    producer.produce(TOPIC, key="XRP_USDT_BATCH", value=json.dumps(result), callback=acked)
    producer.flush()


if __name__ == '__main__':
    pycron.start()