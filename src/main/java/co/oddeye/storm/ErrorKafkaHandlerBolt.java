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

        final OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        final OddeeyMetricMeta mtrsc = (OddeeyMetricMeta) tuple.getValueByField("mtrsc");

        if (LOGGER.isDebugEnabled()) {
//        if (mtrsc.getName().equals("host_absent")) {
            LOGGER.debug(" Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " Time:" + mtrsc.getErrorState().getTime() + " level:" + mtrsc.getErrorState().getLevel() + "Tags:" + mtrsc.getTags() + " Source:" + tuple.getSourceComponent());
        }

//        if (mtrsc.getErrorState().getState() != 1) {
        JsonObject jsonResult = new JsonObject();
        jsonResult.addProperty("hash", mtrsc.hashCode());
        jsonResult.addProperty("key", Hex.encodeHexString(mtrsc.getKey()));
        jsonResult.addProperty("action", mtrsc.getErrorState().getState());
        JsonElement starttimes = gson.toJsonTree(mtrsc.getErrorState().getStarttimes());
        JsonElement values = gson.toJsonTree(mtrsc.LevelValuesList());
        if (time != null) {
            jsonResult.addProperty("time", time);
        }
        if (metric != null) {
            jsonResult.addProperty("time", metric.getTimestamp());
            jsonResult.addProperty("type", metric.getType());
            jsonResult.addProperty("reaction", metric.getReaction());
            jsonResult.addProperty("startvalue", metric.getValue());
            if (metric instanceof OddeeysSpecialMetric) {
                OddeeysSpecialMetric Specmetric = (OddeeysSpecialMetric) metric;
                jsonResult.addProperty("message", Specmetric.getMessage());
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(jsonResult.toString() + " Name:" + metric.getName() + "Host:" + metric.getTags().get("host"));
                }
            }
        }

        jsonResult.add("starttimes", starttimes);
        JsonElement endtimes = gson.toJsonTree(mtrsc.getErrorState().getEndtimes());
        jsonResult.add("endtimes", endtimes);
        jsonResult.addProperty("source", tuple.getSourceComponent());
        jsonResult.addProperty("level", mtrsc.getErrorState().getLevel());
        jsonResult.addProperty("upstate", mtrsc.getErrorState().isUpstate());
//        LOGGER.warn(values.toString());
        jsonResult.add("values", values);
        final ProducerRecord<String, String> datafull = new ProducerRecord<>(mtrsc.getTags().get("UUID").toString() + mtrsc.getErrorState().getLevel(), jsonResult.toString());
        producer.send(datafull);
        if (mtrsc.getErrorState().getLevel() != mtrsc.getErrorState().getPrevlevel()) {
            final ProducerRecord<String, String> dataprev = new ProducerRecord<>(mtrsc.getTags().get("UUID").toString() + mtrsc.getErrorState().getPrevlevel(), jsonResult.toString());
            producer.send(dataprev);
        }

        if (mtrsc.getErrorState().getState() != 1) {
            jsonResult.addProperty("UUID", mtrsc.getTags().get("UUID").toString());
            final ProducerRecord<String, String> data = new ProducerRecord<>(topic, jsonResult.toString());
            producer.send(data);
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Send Data:" + jsonResult.toString() + " Name:" + mtrsc.getName() + "Host:" + mtrsc.getTags().get("host"));
        }
//        }

//        OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
        collector.ack(tuple);
    }

}
