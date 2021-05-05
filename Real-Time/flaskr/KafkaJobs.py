from kafka import KafkaConsumer
import json
#python3 getCurrent.py & python3 getCountryTotal.py & python3 getCountryKafka.py &

def generatemsg(consumer):
    for msg in consumer:
        yield(msg)

def getConsumer(country):
    consumer = KafkaConsumer(country+'Latest',
                            bootstrap_servers=['localhost:9092'],
                            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                            auto_offset_reset='latest',
                            consumer_timeout_ms=3000)
    return consumer

def getGenerator(consumer):
    return generatemsg(consumer)

def getDict(genConsumer,prevdict):
    try:
        ret = next(genConsumer)
        return ret, True


    except StopIteration:
        #print('===================================================================================================')
        return prevdict, False



