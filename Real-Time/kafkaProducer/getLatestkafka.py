import COVID19Py
import time
from sendToTopic import sendToLatestTopic
    
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0
covid19 = COVID19Py.COVID19()
try:
    while True:
        sendToLatestTopic(covid19,producer)
        time.sleep(200)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass