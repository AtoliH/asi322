import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from translate import Translator
import json
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

f=open("config.json","r")
config = json.loads(f.read())
f.close()
# elastic config
es = Elasticsearch(
    "https://localhost:9200",
    ca_certs=config['elastic']['ca_certs'],
    basic_auth=(config['elastic']['user'], config['elastic']['password'])
)
topic = "asi322"
liste_file = "badwords.txt"
consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], client_id=datetime.datetime.now().strftime("%H:%M:%S:f"), group_id="group_asi322")
analyzer=SentimentIntensityAnalyzer()

def send_to_elastic(message):
    resp = es.index(index="asi322", document=message)

if __name__ == "__main__":
    account = config['twitch_bot']['account']
    password = config['twitch_bot']['token']
    headers = {
                "Authorization": "Bearer " + password,
                "Client-Id": config['twitch_bot']['Client_Id']
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
