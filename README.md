## Storm Kafka Client
### build using maven ###
    mvn clean package

### run on storm cluster ###
    storm jar stormkafkaclient-0.0.1-snapshot.jar com.rajkrrsingh.test.KafkaSpoutTopology newKafkaSpout

### to test produce some message to topic sampleinput
   /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list `hostname`:6667 --topic sampleinput

   1,discription,1000,100000,IND

   1,discription,1000,100000,IND
