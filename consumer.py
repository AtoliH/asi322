from kafka import KafkaConsumer
from elasticsearch import ElasticSearch
import json
import re

es = ElasticSearch()
topic = "asi322"
liste_file = "badwords.txt"
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])

def send_to_elastic(message):
    resp = es.index(index="asi322", document=message)

if __name__ == "__main__":
    with open(liste_file, "r") as badwords_file:
        badwords = badwords_file.read().splitlines()
        for message in consumer:
            msg = json.loads(message.value.decode("utf-8"))
            msg['user'] = msg['user'].split('!')[0]
            msg['channel'] = msg['channel'][1:]
            msg['badwords'] = 0
            for word in badwords:
                if re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).search(msg['message']):
                    msg['badwords'] += 1
                    print(word)
            print(msg)

            send_to_elastic(msg)
