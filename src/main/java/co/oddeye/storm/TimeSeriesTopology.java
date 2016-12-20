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

// Specify that the kafka messages are String        
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        kafkaConfig.
//        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
// We want to consume all the first messages in
// the topic every time we run the topology to
// help in debugging. In production, this
// property should be false
//        kafkaConfig.forceFromStart = true;
// set the kafka spout class

        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), Integer.parseInt(String.valueOf(tconf.get("SpoutParallelism_hint"))));
        builder.setSpout("TimerSpout", new TimerSpout(), 1);
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
                .customGrouping("ParseMetricBolt", new MerticGrouper());

        builder.setBolt("CalcRulesBolt",
                new CalcRulesBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CalcRulesBoltParallelism_hint"))))
                .customGrouping("ParseMetricBolt", new MerticGrouper());

        builder.setBolt("ParseSpecialMetricBolt",
                new ParseSpecialMetricBolt(), Integer.parseInt(String.valueOf(tconf.get("ParseMetricBoltParallelism_hint"))))
                .shuffleGrouping("KafkaSpout");
       
        builder.setBolt("CheckSpecialErrorBolt",
                new CheckSpecialErrorBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CheckSpecialErrorBoltParallelism_hint"))))
                .customGrouping("ParseSpecialMetricBolt", new MerticGrouper());


        builder.setBolt("FilterForLastTimeBolt",
                new FilterForLastTimeBolt(), Integer.parseInt(String.valueOf(tconf.get("FilterForLastTimeBoltParallelism_hint"))))
                .shuffleGrouping("ParseSpecialMetricBolt")
                .shuffleGrouping("ParseMetricBolt");

        builder.setBolt("CheckLastTimeBolt",
                new CheckLastTimeBolt(TSDBconfig), Integer.parseInt(String.valueOf(tconf.get("CheckLastTimeBoltParallelism_hint"))))
                .customGrouping("FilterForLastTimeBolt", new MerticGrouper())
                .allGrouping("TimerSpout");
        
        
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
                .shuffleGrouping("CheckSpecialErrorBolt")
                .shuffleGrouping("CheckLastTimeBolt");

//        builder.setBolt("TestBolt",
//                new TestBolt(), Integer.parseInt(String.valueOf(tconf.get("WriteToTSDBseriesParallelism_hint"))))
//                .customGrouping("ParseMetricBolt",new MerticGrouper());
//                .allGrouping("KafkaSpout");
        Config conf = new Config();
        conf.setNumWorkers(Integer.parseInt(String.valueOf(tconf.get("NumWorkers"))));
        conf.put(Config.TOPOLOGY_DEBUG, true);
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