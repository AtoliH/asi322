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
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

Start producer (twitch bot)
===========================
```
python3 bot.py
```
