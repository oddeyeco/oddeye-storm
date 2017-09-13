/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.globalFunctions;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.opentsdb.utils.Config;

/**
 *
 * @author vahan
 */
public class WriteToTSDBseries extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(WriteToTSDBseries.class);
    private final java.util.Map<String, Object> conf;
    private org.hbase.async.Config clientconf;
    private Config openTsdbConfig;

    /**
     *
     * @param config
     */
    public WriteToTSDBseries(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        LOGGER.warn("DoPrepare WriteToTSDBseries");
        collector = oc;

        try {
            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", String.valueOf(conf.get("hbase.rpcs.batch.size")));
            globalFunctions.getTSDB(openTsdbConfig, clientconf);

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");
    }

    @Override
    public void execute(Tuple tuple) {        
        Map<String, OddeeyMetric> MetricList = (Map<String, OddeeyMetric>) tuple.getValueByField("MetricList");
        MetricList.entrySet().stream().map((metricEntry) -> metricEntry.getValue()).forEachOrdered((metric) -> {
            globalFunctions.getTSDB(openTsdbConfig, clientconf).addPoint(metric.getName(), metric.getTimestamp(), metric.getValue(), metric.getTSDBTags());
            
            Date date = new Date(metric.getTimestamp());
            LOGGER.info("Write Metric: "+metric.getName()+" in Time:"+date +" by Value: "+ metric.getValue()+" vs tags: "+ metric.getTSDBTags());
        });
        collector.ack(tuple);
    }
}
