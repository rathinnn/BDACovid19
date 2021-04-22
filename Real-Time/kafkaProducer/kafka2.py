from kafka import KafkaConsumer
from kafka import TopicPartition

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')