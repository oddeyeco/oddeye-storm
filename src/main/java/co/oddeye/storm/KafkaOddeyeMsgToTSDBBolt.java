/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

//import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Properties;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import net.opentsdb.utils.Config;

/**
 *
 * @author vahan
 */
public class KafkaOddeyeMsgToTSDBBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger logger = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger logger = LoggerFactory.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    private TSDB tsdb = null;

    private JsonParser parser = null;
    private JsonArray jsonResult = null;

    @Override
    public void execute(Tuple input) {
        Gson gson = new Gson();  
        String msg = input.getString(0);
        logger.info("Start KafkaOddeyeMsgToTSDBBolt " + msg);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        java.util.Date date = new java.util.Date();
        logger.info("Bolt ready to write to TSDB " + df.format(date.getTime()));
        JsonElement Metric;
        try {
            this.jsonResult = (JsonArray) this.parser.parse(msg);
        } catch (Exception ex) {
            logger.info("msg parse Exception" + ex.toString());
        }

        

//        Map<String, String> myMap = gson.fromJson("{'k1':'apple','k2':'orange'}", type);        
//        ObjectMapper mapper= new ObjectMapper();
        HashMap<String,String> tags = new HashMap<String,String>();
        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    logger.info("Ready count: " + this.jsonResult.size());
                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        if (Metric.getAsJsonObject().get("tags") != null) {
                            tags= gson.fromJson(Metric.getAsJsonObject().get("tags").getAsJsonObject(), tags.getClass());
                            tsdb.addPoint(Metric.getAsJsonObject().get("metric").getAsString(), Metric.getAsJsonObject().get("timestamp").getAsLong(), Metric.getAsJsonObject().get("value").getAsDouble(), tags);                            
                            tags.clear();
                        }                        
                    }
                    logger.info(this.jsonResult.size() + " Metrics Write to dbase");
                }
            } catch (Exception ex) {
                logger.info("JSON parse Exception" + ex.toString());
            }
        }
        this.collector.ack(input);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {
        try {
            this.tsdb.shutdown().joinUninterruptibly();
        } catch (Exception ex) {
            logger.error("OpenTSDB shutdown execption : " + ex.toString());
            throw new RuntimeException(ex);
        }
    }

    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        logger.info("DoPrepare KafkaOddeyeMsgToTSDBBolt");
        this.collector = collector;
        this.parser = new JsonParser();
        Properties tsdbConf = new Properties();
        try {
            tsdbConf.put("hbase.zookeeper.quorum", "nn1.netangels.net:2181,nn2.netangels.net:2181,rm1.netangels.net:2181");
            tsdbConf.put("tsd.core.auto_create_metrics", "true");
            tsdbConf.put("tsd.storage.hbase.data_table", "test_tsdb");
            tsdbConf.put("tsd.storage.hbase.uid_table", "test_tsdb-uid");

            String quorum = tsdbConf.getProperty("hbase.zookeeper.quorum", "localhost");

            Config openTsdbConfig = new net.opentsdb.utils.Config(true);

            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", "true");
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", "test_tsdb");
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", "test_tsdb-uid");

            org.hbase.async.HBaseClient client = new org.hbase.async.HBaseClient(quorum);
            this.tsdb = new TSDB(
                    client,
                    openTsdbConfig);

        } catch (IOException ex) {
            logger.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            logger.error("OpenTSDB config execption : " + ex.toString());
        }

        logger.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");

    }
}
