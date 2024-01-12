from confluent_kafka import Producer

from websockets.sync.client import connect
import websockets
import json

TOPIC = 'xrp-usdt-stream'

conf = {'bootstrap.servers': 'kafka:9092',
        'client.id': "CRYPTO_XRP_USDT_SOCKET_PRODUCER"}


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


producer = Producer(conf)

with connect("wss://stream.crypto.com/exchange/v1/market") as websocket:
    data = {
      "id": 1,
      "method": "subscribe",
      "params": {
          "channels": ["trade.XRP_USDT"]
      }
    }
    websocket.send(json.dumps(data))

    while True:
        try:
            message = websocket.recv()
        except websockets.ConnectionClosedOK:
            break

        payload = json.loads(message)
        # print(json.dumps(payload, indent=2))
        if payload['method'] == "public/heartbeat":
            heartbeat = {
                "id": payload['id'],
                "method": "public/respond-heartbeat"
            }
            print('Replied heartbeat: ', heartbeat)
            websocket.send(json.dumps(heartbeat))
        else:
            print('Sent payload~ :' + json.dumps(payload))
            producer.produce(TOPIC, key="XRP_USDT", value=json.dumps(payload), callback=acked)

