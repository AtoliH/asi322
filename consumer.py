from kafka import KafkaConsumer

topic="asi322"
liste="badwords.txt"
consumer = KafkaConsumer(topic,
                         bootstrap_servers=['localhost:9092'])

for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))