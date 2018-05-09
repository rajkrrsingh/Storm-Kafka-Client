## Storm Kafka Client
### build using maven ###
    mvn clean package

### run on storm cluster ###
    storm jar /tmp/StormKafkaClient-0.0.1-SNAPSHOT.^Cr com.rajkrrsingh.test.KafkaSpoutTopology c421-node4.sandy.com:6667
### to test produce some message to topics

    /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list c421-node4.sandy.com:6667 --messages 100 --initial-message-id 0001 --topics kafka-spout-test-1

    /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list c421-node4.sandy.com:6667 --messages 100 --initial-message-id 0001 --topics kafka-spout-test-2

    /usr/hdp/current/kafka-broker/bin/kafka-producer-perf-test.sh --broker-list c421-node4.sandy.com:6667 --messages 100 --initial-message-id 0001 --topics kafka-spout-test

