/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ParseMetricBolt extends BaseRichBolt {

    protected OutputCollector collector;
    public static final Logger LOGGER = LoggerFactory.getLogger(ParseMetricBolt.class);
    private JsonParser parser = null;
    private JsonArray jsonResult = null;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("metric"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        LOGGER.warn("DoPrepare ParseMetricBolt");
        collector = oc;
        parser = new JsonParser();
    }

    @Override
    public void execute(Tuple input) {
        String msg = input.getString(0);
        LOGGER.debug("Start messge:" + msg);
        this.collector.ack(input);
        JsonElement Metric;
        try {
            if (this.parser.parse(msg).isJsonArray()) {
                this.jsonResult = this.parser.parse(msg).getAsJsonArray();
            } else {
                this.jsonResult = null;
                LOGGER.error("not array:" + msg);
            }
        } catch (JsonSyntaxException ex) {
            LOGGER.info("msg parse Exception" + ex.toString());
        }
        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    LOGGER.debug("Ready count: " + this.jsonResult.size());
                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        if (Metric.getAsJsonObject().get("specialTag") != null && Metric.getAsJsonObject().get("specialTag").getAsBoolean()) {
                            LOGGER.info("Welcom special tag:" + Metric.toString());
                            continue;
                        }
                        try {
                            final OddeeyMetric mtrsc = new OddeeyMetric(Metric);
                            if (mtrsc.getName() == null) {
                                LOGGER.warn("mtrsc.getName()==null " + Metric);
                                LOGGER.warn("mtrsc.getName()==null " + msg);
                                continue;
                            }
                            if (mtrsc.getTimestamp() == null) {
                                LOGGER.warn("mtrsc.getTimestamp()==null " + Metric);
                                LOGGER.warn("mtrsc.getTimestamp()==null " + msg);
                                continue;
                            }
                            if (mtrsc.getValue() == null) {
                                LOGGER.warn("mtrsc.getValue()==null " + Metric);
                                LOGGER.warn("mtrsc.getValue()==null " + msg);
                                continue;
                            }
                            if (mtrsc.getTSDBTags() == null) {
                                LOGGER.warn("mtrsc.getTSDBTags()==null " + Metric);
                                LOGGER.warn("mtrsc.getTSDBTags()==null " + msg);
                                continue;
                            }
                            collector.emit(new Values(mtrsc));

                        } catch (Exception e) {
                            LOGGER.error("Exception: " + globalFunctions.stackTrace(e));
                            LOGGER.error("Exception Wits Metriq: " + Metric);
                            LOGGER.error("Exception Wits Input: " + msg);
                        }

                    }
                }
            } catch (JsonSyntaxException ex) {
                LOGGER.error("JsonSyntaxException: " + globalFunctions.stackTrace(ex));
//                this.collector.ack(input);
            } catch (NumberFormatException ex) {
                LOGGER.error("NumberFormatException: " + globalFunctions.stackTrace(ex));
//                this.collector.ack(input);
            }
            this.jsonResult = null;
        }
    }

}
