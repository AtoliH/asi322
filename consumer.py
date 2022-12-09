from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from translate import Translator
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
    account = "asi322"
    password = "08q7pnik40y1x5lgaxlkmdeioa4t2f"
    headers = {
                "Authorization": "Bearer " + password,
                "Client-Id": "0ahgkrb8ju27rj3xl01iin1emkixne"
            }

    with open(liste_file, "r") as badwords_file:
        print("Waiting for producer...")
        badwords = badwords_file.read().splitlines()
        for message in consumer:
            msg = json.loads(message.value.decode("utf-8"))
            msg['user'] = msg['user'].split('!')[0]
            msg['channel'] = msg['channel'][1:]

            # Find badwords in message
            msg['badwords'] = []
            for word in badwords:
                if re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).search(msg['message']):
                    msg['badwords'].append(word)

            # Translate message to en
            translator = Translator(to_lang="en")
            translation = translator.translate(msg['message'])
            msg['translate_en'] = translation

            vs = analyzer.polarity_scores(msg['message'])
            msg['vs'] = vs
            send_to_elastic(msg)
            print(msg)
