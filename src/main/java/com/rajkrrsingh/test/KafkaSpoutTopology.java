package com.rajkrrsingh.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * Created by rasingh on 3/6/17.
 */
public class KafkaSpoutTopology {
    private static final String TOPIC_STREAM = "test_stream";
    private static final String[] TOPICS = new String[]{"sampleinput","sampleoutput"};



    public static void main(String[] args) throws Exception{
        if (args.length == 0) {
            submitTopologyLocalCluster(getTopologyKafkaSpout(), getConfig());
        } else {
            submitTopologyRemoteCluster(args[0], getTopologyKafkaSpout(), getConfig());
        }
    }

    public static void submitTopologyRemoteCluster(String arg, StormTopology topology, Config config) throws Exception {
        StormSubmitter.submitTopology(arg, config, topology);
    }

    public   static void submitTopologyLocalCluster(StormTopology topology, Config config) throws Exception {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, topology);
        stopWaitingForInput();
    }

    protected static StormTopology getTopologyKafkaSpout() {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig()), 1);
        tp.setBolt("kafka_bolt", new KafkaSpoutTestBolt())
                .shuffleGrouping("kafka_spout", TOPIC_STREAM);
        return tp.createTopology();
    }

    protected static void stopWaitingForInput() {
        try {
            System.out.println("PRESS ENTER TO STOP");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            System.exit(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static KafkaSpoutConfig<String,String> getKafkaSpoutConfig() {

        String[] STREAMS = new String[]{"test_stream", "test1_stream", "test2_stream"};
        String[] TOPICS = new String[]{"sampleinput", "samplepleoutput", "sampleinput"};

        Map<String, Object> kafkaConsumerProps= new HashMap<>();
        kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.BOOTSTRAP_SERVERS,"noden3:6667");
        kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.GROUP_ID,"test");
        kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.KEY_DESERIALIZER,"org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put(KafkaSpoutConfig.Consumer.VALUE_DESERIALIZER,"org.apache.kafka.common.serialization.StringDeserializer");


        KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(new KafkaSpoutRetryExponentialBackoff.TimeInterval(500, TimeUnit.MICROSECONDS),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));

        Fields outputFields = new Fields("topic", "partition", "offset", "key", "value");
        Fields outputFields1 = new Fields("topic", "partition", "offset");

        KafkaSpoutStreams kafkaSpoutStreams = new KafkaSpoutStreamsNamedTopics.Builder(outputFields, STREAMS[0], new String[]{TOPICS[0], TOPICS[1]})
                .addStream(outputFields, STREAMS[0], new String[]{TOPICS[2]})  // contents of topic test2 sent to test_stream
                .addStream(outputFields1, STREAMS[2], new String[]{TOPICS[2]})  // contents of topic test2 sent to test2_stream
                .build();

        KafkaSpoutTuplesBuilder tuplesBuilder = new KafkaSpoutTuplesBuilder() {
            @Override
            public List<Object> buildTuple(ConsumerRecord consumerRecord) {
                return new Values(consumerRecord.topic(),consumerRecord.partition(),consumerRecord.offset(),consumerRecord.key(),consumerRecord.value());
            }
        };


        KafkaSpoutConfig.Builder kafkaSpoutConfig = new KafkaSpoutConfig.Builder<String, String>(kafkaConsumerProps, kafkaSpoutStreams, tuplesBuilder, retryService)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250);

        return kafkaSpoutConfig.build();
    }

    protected  static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        return config;
    }

}
