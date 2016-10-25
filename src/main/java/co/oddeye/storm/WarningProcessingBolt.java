/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Map;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class WarningProcessingBolt extends BaseRichBolt {

    private final java.util.Map<String, Object> conf;
    public static final Logger LOGGER = LoggerFactory.getLogger(WarningProcessingBolt.class);

    private byte[] errortable;
    private final byte[] error_family = "d".getBytes();
    private OutputCollector collector;
    private OddeeyMetricMeta metric;
    private Short weight;
    private Calendar calendarObj;
    private byte[] key;
    private Config clientconf;
    private static HBaseClient client;

    public WarningProcessingBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector i_collector) {
        this.errortable = String.valueOf(conf.get("errorstable")).getBytes();
        collector = i_collector;

        String quorum = String.valueOf(conf.get("zkHosts"));
        clientconf = new org.hbase.async.Config();
        clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
        clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");
        while (WarningProcessingBolt.client == null) {
            try {
                WarningProcessingBolt.client = new org.hbase.async.HBaseClient(clientconf);
            } catch (Exception e) {
                LOGGER.warn("HBaseClient Connection fail in WarningProcessingBolt");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }

    }

    private String stackTrace(Exception cause) {
        if (cause == null) {
            return "-/-";
        }
        StringWriter sw = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    @Override
    public void execute(Tuple input) {
        metric = (OddeeyMetricMeta) input.getValueByField("metric");
        weight = input.getShortByField("weight");
        calendarObj = (Calendar) input.getValueByField("calendar");
        LOGGER.info("Strat warning bolt input weight = " + weight + " Date: " + calendarObj.getTime() + " metric Name:" + metric.getName() + " tags:" + metric.getTags());
//        LOGGER.info("Strat warning bolt input weight = " + weight+ " Date: "+calendarObj.getTime());        
//        key = ByteBuffer.allocate(8).putLong((long) (calendarObj.getTimeInMillis()/1000)).array(); 

        key = metric.getTags().get("UUID").getValueTSDBUID();
        key = ArrayUtils.addAll(key, ByteBuffer.allocate(8).putLong((long) (calendarObj.getTimeInMillis()/1000)).array());        
        PutRequest putvalue = new PutRequest(errortable, key, error_family, metric.getKey(), ByteBuffer.allocate(2).putShort(weight).array());
        client.put(putvalue);

        this.collector.ack(input);
    }

}
