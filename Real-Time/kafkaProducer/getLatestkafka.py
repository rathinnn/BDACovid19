import COVID19Py
import time
covid19 = COVID19Py.COVID19()
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0

try:
    while True:
        latest = covid19.getLatest()
        producer.send('TutorialTopic' , latest)
        time.sleep(200)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass