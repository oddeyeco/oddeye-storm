/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;

import java.io.FileInputStream;
import java.io.InputStreamReader;
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
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 *
 * @author vahan
 */
public class KafkaHbaseTopology {

    public static void main(String[] args) {
        String Filename = args[0];
        Config topologyconf = new Config();
        try {
            Yaml yaml = new Yaml();
            java.util.Map rs = (java.util.Map) yaml.load(new InputStreamReader(new FileInputStream(Filename)));
            topologyconf.putAll(rs);
        } catch (Exception e) {
            System.out.println(e);
        }

        java.util.Map<String, Object> kafkaconf = (java.util.Map<String, Object>) topologyconf.get("Kafka");
        java.util.Map<String, Object> tconf = (java.util.Map<String, Object>) topologyconf.get("Topology");

        TopologyBuilder builder = new TopologyBuilder();
//        HBaseBolt
// zookeeper hosts for the Kafka cluster
        BrokerHosts zkHosts = new ZkHosts(String.valueOf(kafkaconf.get("zkHosts")));
// Create the KafkaSpout configuration
// Second argument is the topic name
// Third argument is the ZooKeeper root for Kafka
// Fourth argument is consumer group id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,
                String.valueOf(kafkaconf.get("topic")), String.valueOf(kafkaconf.get("zkRoot")), String.valueOf(kafkaconf.get("zkKey")));
// Specify that the kafka messages are String        
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
// We want to consume all the first messages in
// the topic every time we run the topology to
// help in debugging. In production, this
// property should be false
//        kafkaConfig.forceFromStart = true;
// set the kafka spout class

        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutParallelism_hint"))));

        builder.setBolt("KafkaOddeyeMsgToHbaseBolt",
                new KafkaOddeyeMsgToHbaseBolt(), Integer.parseInt(String.valueOf(tconf.get("MsgBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");
        
        builder.setBolt("KafkaOddeyeMetaToHbaseBolt",
                new KafkaOddeyeMetaToHbaseBolt(), Integer.parseInt(String.valueOf(tconf.get("MetaBoltParallelism_hint"))))
                .fieldsGrouping("KafkaOddeyeMsgToHbaseBolt", new Fields("json"));
//                .shuffleGrouping("KafkaOddeyeMsgToHbaseBolt");
        
        

//        builder.setBolt("WriteHbase", hbase, 2).fieldsGrouping("KaftaToJsonBolt", new Fields("word"));
        Config conf = new Config();
        conf.setNumWorkers(Integer.parseInt(String.valueOf(tconf.get("NumWorkers"))));
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setDebug(Boolean.getBoolean(String.valueOf(tconf.get("Debug"))));
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
