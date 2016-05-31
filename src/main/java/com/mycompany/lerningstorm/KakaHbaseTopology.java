/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.lerningstorm;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author vahan
 */
public class KakaHbaseTopology {

    public static void main(String[] args) {
//        try {
//            System.out.println("all versions of log4j Logger: " + KafkaSpout.class.getClassLoader().getResources("org/apache/log4j/Logger.class"));
//
//            System.out.println("all versions of XMLConfigurator: " + KafkaSpout.class.getClassLoader().getResources("org/opensaml/xml/XMLConfigurator.class"));
//
//            System.out.println("Pogos2");
//
//        } catch (Exception e) {
//            System.out.println(e);
//        }

        TopologyBuilder builder = new TopologyBuilder();

// zookeeper hosts for the Kafka cluster
        BrokerHosts zkHosts = new ZkHosts("192.168.10.50:2181");
// Create the KafkaSpout configuration
// Second argument is the topic name
// Third argument is the ZooKeeper root for Kafka
// Fourth argument is consumer group id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,
                "oddeye", "", "id7");
// Specify that the kafka messages are String        

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
// We want to consume all the first messages in
// the topic every time we run the topology to
// help in debugging. In production, this
// property should be false
//        kafkaConfig.forceFromStart = true;
// set the kafka spout class
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 2);

//// set the spout class
//        builder.setSpout("LearningStormSpout",
//                new LearningStormSpout(), 4);
// set the bolt class
        
//        builder.setBolt("LearningStormBolt",
//                new LearningStormBolt(), 2)
//                .shuffleGrouping("KafkaSpout");
        
//        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
//                .withRowKeyField("word")
//                .withColumnFields(new Fields("word"))
//                .withCounterFields(new Fields("count"))
//                .withColumnFamily("cf");        
//        
//        HBaseBolt hbase = new HBaseBolt("oddeyedata", mapper);
        
        builder.setBolt("KaftaToJsonBolt",
                new KaftaToJsonBolt(), 2)
                .shuffleGrouping("KafkaSpout");  
        
//        builder.setBolt("WriteHbase", hbase, 2).fieldsGrouping("KaftaToJsonBolt", new Fields("word"));
        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setDebug(true);
        try {
// This statement submit the topology on remote cluster. // args[0] = name of topology StormSubmitter.
            StormSubmitter.submitTopology("KakaHbaseTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException alreadyAliveException) {
            System.out.println(alreadyAliveException);
        } catch (InvalidTopologyException invalidTopologyException) {
            System.out.println(invalidTopologyException);
        } catch (AuthorizationException invalidAuthorizationException) {
            System.out.println(invalidAuthorizationException);
        }
    }
}
