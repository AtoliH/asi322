Dependencies
============
kafka
```
https://kafka.apache.org/downloads
```
Python modules
```
pip install kafka-python
pip install websockets
pip install elasticsearch
pip install requests
pip install vaderSentiment
```
Elastic Stack
```
https://www.elastic.co/guide/en/elastic-stack/current/installing-elastic-stack.html
```

Start Elastic Stack
```
./elasticsearch/bin/elasticsearch
```

Start kafka
===========
```
./kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties
./kafka/bin/kafka-server-start.sh kafka/config/server.properties
./kafka/bin/kafka-topics.sh --create --topic asi322 --bootstrap-server localhost:9092 --partitions 32 --replication-factor 1
```

Start consumers
==============
```
for i in {1..32}; do python3 consumer.py > log_consumer$i.txt 2>&1 &; done
```

Start test consumer
```
./kafka/bin/kafka-console-consumer.sh --topic asi322 --from-beginning --bootstrap-server localhost:9092
python3 consumer.py
```

Start producer (twitch bot)
===========================
```
python3 bot.py
```
