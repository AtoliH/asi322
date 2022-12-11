import sys
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from translate import Translator
import json
import re
import os
import traceback
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


def send_to_elastic(es, message):
    resp = es.index(index="asi322", document=message)


def main():
    print("PID: " + str(os.getpid()))

    f = open("config.json", "r")
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
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], client_id=sys.argv[1],
                             group_id="group_asi322")
    analyzer = SentimentIntensityAnalyzer()

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
            msg['channel'] = msg['channel']

            if msg['details']['broadcaster_language'] != "en":
                # Translate message to en
                translator = Translator(provider="Google", to_lang="en", from_lang=msg['details']['broadcaster_language'])
                translation = translator.translate(msg['message'])
                msg['translate_en'] = translation
            else:
                msg['translate_en'] = msg['message']

            # Find badwords in message
            msg['badwords'] = []
            for word in badwords:
                if re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).search(msg['translate_en']):
                    msg['badwords'].append(word)

            vs = analyzer.polarity_scores(msg['translate_en'])
            msg['vs'] = vs
            send_to_elastic(es, msg)
            print(msg)


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception:
            print(traceback.format_exc())
