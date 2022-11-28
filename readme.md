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
pip install vaderSentiment
```
Elastic Stack
```
https://www.elastic.co/guide/en/elastic-stack/current/installing-elastic-stack.html
```

Start Elastic Stack
```
.elasticsearch/bin/elasticsearch
```

Start kafka
===========
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic asi322 --bootstrap-server localhost:9092
```

Start consumer
==============
```
bin/kafka-console-consumer.sh --topic asi322 --from-beginning --bootstrap-server localhost:9092
```

Start producer (twitch bot)
===========================
```
python3 bot.py
```
