/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Properties;
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

/**
 *
 * @author vahan
 */
public class TimeSeriesTopology {

    private static String topologyname;

    public static void main(String[] args) {
        String Filename = args[0];
        Config topologyconf = new Config();
        try {
            Yaml yaml = new Yaml();
            java.util.Map rs = (java.util.Map) yaml.load(new InputStreamReader(new FileInputStream(Filename)));
            topologyconf.putAll(rs);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }

        java.util.Map<String, Object> kafkaconf = (java.util.Map<String, Object>) topologyconf.get("Kafka");
        java.util.Map<String, Object> kafkasemaphoreconf = (java.util.Map<String, Object>) topologyconf.get("KafkaSemaphore");
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
                String.valueOf(kafkaconf.get("tsdbtopic")), String.valueOf(kafkaconf.get("zkRoot")), String.valueOf(kafkaconf.get("zkKeyTSDB")));

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutParallelism_hint"))));
        builder.setSpout("TimerSpout", new TimerSpout(), 1);
        builder.setSpout("TimerSpout2x", new TimerSpout2x(), 1);
        // Semaphore Spout        
        BrokerHosts zkSemaphoreHosts = new ZkHosts(String.valueOf(kafkasemaphoreconf.get("zkHosts")));

        SpoutConfig kafkaSemaphoreConfig = new SpoutConfig(zkSemaphoreHosts,
                String.valueOf(kafkasemaphoreconf.get("topic")), String.valueOf(kafkasemaphoreconf.get("zkRoot")), String.valueOf(kafkasemaphoreconf.get("zkKey")));

        kafkaSemaphoreConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        builder.setSpout("kafkaSemaphoreSpot", new KafkaSpout(kafkaSemaphoreConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutSemaphoreParallelism_hint"))));

        /*
        //Disable hbase bolts
        builder.setBolt("KafkaOddeyeMsgToHbaseBolt",
                new KafkaOddeyeMsgToHbaseBolt(), Integer.parseInt(String.valueOf(tconf.get("MsgBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");
        
        builder.setBolt("KafkaOddeyeMetaToHbaseBolt",
                new KafkaOddeyeMetaToHbaseBolt(), Integer.parseInt(String.valueOf(tconf.get("MetaBoltParallelism_hint"))))
                .fieldsGrouping("KafkaOddeyeMsgToHbaseBolt", new Fields("json"));
//                .shuffleGrouping("KafkaOddeyeMsgToHbaseBolt");
         */
        java.util.Map<String, Object> TSDBconfig = (java.util.Map<String, Object>) topologyconf.get("Tsdb");
        boolean CheckDisabled;
        CheckDisabled = Boolean.valueOf(String.valueOf(tconf.get("DisableCheck")));

        TSDBconfig.put("DisableCheck", Boolean.toString(CheckDisabled));

        builder.setBolt("ParseMetricBolt",
                new ParseMetricBolt(), Integer.parseInt(String.valueOf(tconf.get("ParseMetricBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");

        builder.setBolt("WriteToTSDBseries",
                new WriteToTSDBseries(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("WriteToTSDBseriesParallelism_hint"))))
                .shuffleGrouping("ParseMetricBolt");

        builder.setBolt("CompareBolt",
                new CompareBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CompareBoltParallelism_hint"))))
                .customGrouping("ParseMetricBolt", new MerticGrouper())
                .allGrouping("SemaforProxyBolt");

        builder.setBolt("CalcRulesBolt",
                new CalcRulesBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CalcRulesBoltParallelism_hint"))))
                .customGrouping("ParseMetricBolt", new MerticGrouper())
                .allGrouping("SemaforProxyBolt");

        builder.setBolt("ParseSpecialMetricBolt",
                new ParseSpecialMetricBolt(), Integer.parseInt(String.valueOf(tconf.get("ParseMetricBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");

        builder.setBolt("SemaforProxyBolt",
                new SemaforProxyBolt(), Integer.parseInt(String.valueOf(tconf.get("SemaforProxyBoltParallelism_hint"))))
                .shuffleGrouping("kafkaSemaphoreSpot");

        builder.setBolt("CheckSpecialErrorBolt",
                new CheckSpecialErrorBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CheckSpecialErrorBoltParallelism_hint"))))
                .customGrouping("ParseSpecialMetricBolt", new MerticGrouper())
                .allGrouping("TimerSpout")
                .allGrouping("SemaforProxyBolt");

        java.util.Map<String, Object> Mailconfig = (java.util.Map<String, Object>) topologyconf.get("mail");

        builder.setBolt("SendNotifierBolt",
                new SendNotifierBolt(TSDBconfig, Mailconfig), Integer.parseInt(String.valueOf(tconf.get("SendNotifierBoltParallelism_hint"))))                
                .customGrouping("CompareBolt",new MetaByUserGrouper())
                .customGrouping("CheckSpecialErrorBolt",new MetaByUserGrouper())                
                .allGrouping("TimerSpout2x")
                .allGrouping("kafkaSemaphoreSpot");   
        builder.setBolt("MetricErrorToHbase",
                new MetricErrorToHbase(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("MetricErrorToHbaseParallelism_hint"))))
                .shuffleGrouping("CompareBolt")
                .shuffleGrouping("CheckSpecialErrorBolt");

        java.util.Map<String, Object> errorKafkaConf = (java.util.Map<String, Object>) topologyconf.get("ErrorKafka");
        Properties props = new Properties();
        props.put("bootstrap.servers", String.valueOf(errorKafkaConf.get("bootstrap.servers")));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = String.valueOf(errorKafkaConf.get("topic"));
        builder.setBolt("ErrorKafkaHandlerBolt",
                new ErrorKafkaHandlerBolt(props,topic), Integer.parseInt(String.valueOf(tconf.get("ErrorKafkaHandlerParallelism_hint"))))
                .shuffleGrouping("CompareBolt")
                .shuffleGrouping("CheckSpecialErrorBolt");
////                .shuffleGrouping("CheckLastTimeBolt");

        Config conf = new Config();
        conf.setNumWorkers(Integer.parseInt(String.valueOf(tconf.get("NumWorkers"))));
//        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.setMaxSpoutPending(Integer.parseInt(String.valueOf(tconf.get("topology.max.spout.pending"))));
        conf.setDebug(Boolean.getBoolean(String.valueOf(tconf.get("Debug"))));
        conf.setMessageTimeoutSecs(Integer.parseInt(String.valueOf(tconf.get("topology.message.timeout.secs"))));
        try {
// This statement submit the topology on remote cluster. // args[0] = name of topology StormSubmitter.
            topologyname = String.valueOf(tconf.get("topologi.display.name"));
            if (CheckDisabled) {
                topologyname = topologyname + "_NoCheck";
            }
            StormSubmitter.submitTopology(topologyname, conf, builder.createTopology());
        } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException alreadyAliveException) {
            System.out.println(alreadyAliveException);
        }
    }
}
