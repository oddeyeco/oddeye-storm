/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author vahan
 */
public class KafkaHbaseTopology {

    public static void main(String[] args) {
                                
        TopologyBuilder builder = new TopologyBuilder();
//        HBaseBolt
// zookeeper hosts for the Kafka cluster
        BrokerHosts zkHosts = new ZkHosts("nn1.netangels.net:2181,nn2.netangels.net:2181,rm1.netangels.net:2181");
// Create the KafkaSpout configuration
// Second argument is the topic name
// Third argument is the ZooKeeper root for Kafka
// Fourth argument is consumer group id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,
                "oddeye", "/storm_oddeye", "oddeye-staus");
// Specify that the kafka messages are String        
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
// We want to consume all the first messages in
// the topic every time we run the topology to
// help in debugging. In production, this
// property should be false
//        kafkaConfig.forceFromStart = true;
// set the kafka spout class
        
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 12);
        
        builder.setBolt("KafkaOddeyeMsgToHbaseBolt",
                new KafkaOddeyeMsgToHbaseBolt(), 12)
                .shuffleGrouping("KafkaSpout");  
        
//        builder.setBolt("WriteHbase", hbase, 2).fieldsGrouping("KaftaToJsonBolt", new Fields("word"));
        Config conf = new Config();
        conf.setNumWorkers(12);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setDebug(true);
        try {
// This statement submit the topology on remote cluster. // args[0] = name of topology StormSubmitter.
            StormSubmitter.submitTopology("KafkaHbaseTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException alreadyAliveException) {
            System.out.println(alreadyAliveException);
        } catch (InvalidTopologyException invalidTopologyException) {
            System.out.println(invalidTopologyException);
        } catch (AuthorizationException invalidAuthorizationException) {
            System.out.println(invalidAuthorizationException);
        }
    }
}
