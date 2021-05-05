from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('IndiaLatest',
                         bootstrap_servers=['localhost:9092'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         auto_offset_reset='latest',
                         )#consumer_timeout_ms=3000)

def generatemsg(consumer):
    for msg in consumer:
        yield(msg)

x = generatemsg(consumer)
while(True):    
    print(next(x))



