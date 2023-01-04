#!/bin/bash

# 1) start zookeeper
zookeeper-server-start.sh ~/kafka-3.3.1-src/config/zookeeper.properties

# 2) start kafka-server
kafka-server-start.sh ~/kafka-3.3.1-src/config/server.properties
# it create bootstrap-server on localhost:9092
# if error - clear logDir :)

# 3) create topic with name - 'twitter_tweets'
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter_tweets --partitions 6 --replication-factor 1

# 4 (optional) ) create producer
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic twitter_tweets

# 5 (optional) ) create consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets
