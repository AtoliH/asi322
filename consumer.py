from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import requests
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
    password = "qz0c0xytsxsqe9die1n9k5kb0bd51o"
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

            # Find badwords in message
            msg['badwords'] = []
            for word in badwords:
                if re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).search(msg['message']):
                    msg['badwords'].append(word)

            # Get channel info (too slow)
            channel = msg['channel'][1:]
            url = "https://api.twitch.tv/helix/users?login=" + str(channel)
            channel_id = requests.get(url, headers=headers).json()["data"][0]["id"]
            url = "https://api.twitch.tv/helix/channels?broadcaster_id=" + str(channel_id)
            channel_data = requests.get(url, headers=headers).json()["data"][0]
            msg['channel'] = channel_data

            vs = analyzer.polarity_scores(msg['message'])
            msg['vs'] = vs
            send_to_elastic(msg)
            print(msg)
