/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeysSpecialMetric;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ErrorKafkaHandlerBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(ErrorKafkaHandlerBolt.class);
    private final Properties conf;
    private final String topic;
    private KafkaProducer<String, String> producer;
    private Gson gson;

    /**
     *
     * @param config
     * @param s_topic
     */
    public ErrorKafkaHandlerBolt(Properties config, String s_topic) {
        this.conf = config;
        this.topic = s_topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
        collector = oc;
        producer = new KafkaProducer<>(conf);
        gson = new Gson();
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void execute(Tuple tuple) {
//        LOGGER.warn("getFields count = " + tuple.getFields().size());
        Long time = null;

        if (tuple.getFields().contains("time")) {
            time = (Long) tuple.getValueByField("time");
        }

        OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        OddeeyMetricMeta mtrsc = (OddeeyMetricMeta) tuple.getValueByField("mtrsc");

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(" Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " level:" + mtrsc.getErrorState().getLevel() + "Tags:" + mtrsc.getTags());
        }
        if (mtrsc.getErrorState().getState() != 1) {
            JsonObject jsonResult = new JsonObject();
            jsonResult.addProperty("hash", mtrsc.hashCode());
            jsonResult.addProperty("key", Hex.encodeHexString(mtrsc.getKey()));
            jsonResult.addProperty("UUID", mtrsc.getTags().get("UUID").toString());
            jsonResult.addProperty("level", mtrsc.getErrorState().getLevel());
            jsonResult.addProperty("action", mtrsc.getErrorState().getState());
            if (tuple.getSourceComponent().equals("CheckLastTimeBolt")) {
                jsonResult.addProperty("type", "Special");
            } else {
                jsonResult.addProperty("type", "Regular");
            }
            JsonElement starttimes = gson.toJsonTree(mtrsc.getErrorState().getStarttimes());

            if (metric != null) {
                jsonResult.addProperty("time", metric.getTimestamp());
                if (metric instanceof OddeeysSpecialMetric) {
                    OddeeysSpecialMetric Specmetric = (OddeeysSpecialMetric) metric;
                    jsonResult.addProperty("message", Specmetric.getMessage());
                    jsonResult.addProperty("type", "Special");
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(jsonResult.toString() + " Name:" + metric.getName() + "Host:" + metric.getTags().get("host"));
                    }
                }
            }
            if (mtrsc.getName().equals("host_absent")) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(mtrsc.getErrorState().getTime());
                jsonResult.addProperty("message", "Host Absent by " + cal.getTime());
                jsonResult.addProperty("type", "Special");
            }
            if (time != null) {
                jsonResult.addProperty("time", time);
            }
            jsonResult.add("starttimes", starttimes);
            JsonElement endtimes = gson.toJsonTree(mtrsc.getErrorState().getEndtimes());
            jsonResult.add("endtimes", endtimes);
            jsonResult.addProperty("source", tuple.getSourceComponent());
            final ProducerRecord<String, String> data = new ProducerRecord<>(topic, jsonResult.toString());
            producer.send(data);
//            if (metric != null) {
//                LOGGER.warn("Source:" + tuple.getSourceComponent() + " Name:" + metric.getName() + " spec:" + Boolean.toString((metric instanceof OddeeysSpecialMetric))+" data:"+jsonResult.toString());
//            }

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Send Data:" + jsonResult.toString() + " Name:" + mtrsc.getName() + "Host:" + mtrsc.getTags().get("host"));
            }
        }

//        OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        collector.ack(tuple);
    }

}
