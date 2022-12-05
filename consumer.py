from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# elastic config
es = Elasticsearch(
    "https://localhost:9200",
    ca_certs="./elasticsearch-8.5.2/config/certs/http_ca.crt",
    basic_auth=("elastic", "elastic")
)
topic = "asi322"
liste_file = "badwords.txt"
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
analyzer=SentimentIntensityAnalyzer()

def send_to_elastic(message):
    resp = es.index(index="asi322", document=message)

if __name__ == "__main__":
    with open(liste_file, "r") as badwords_file:
        print("Initialization success !")
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

            vs=analyzer.polarity_scores(msg['message'])
            msg['vs']=vs
            send_to_elastic(msg)
            print(msg)
