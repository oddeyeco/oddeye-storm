/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeysSpecialMetric;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
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
public abstract class BaseParseMetricBolt extends BaseRichBolt {

    private Date date;
    protected Boolean special;
    private JsonParser parser;
    public static final Logger baseLOGGER = LoggerFactory.getLogger(BaseParseMetricBolt.class);
    protected OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("MetricField"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        baseLOGGER.warn("DoPrepare " + getClass());
        collector = oc;
        parser = new JsonParser();
    }

//    public BaseParseMetricBolt() {
//        parser = new JsonParser();
//    }
    protected TreeMap<Integer, OddeeyMetric> prepareJsonObjectV2(JsonElement json, String msg) {
        JsonObject jsonResult = json.getAsJsonObject().get("data").getAsJsonObject();

        if (jsonResult.size() > 0) {
            baseLOGGER.debug("Ready count: " + jsonResult.size());
            final TreeMap<Integer, OddeeyMetric> MetricList = new TreeMap<>();

//            for (int i = 0; i < jsonResult.size(); i++) 
            for (Map.Entry<String, JsonElement> JsonEntry : jsonResult.entrySet()) {
                JsonElement tmpMetric = JsonEntry.getValue();
                JsonObject Metric = new JsonObject();

                Metric.add("tags", json.getAsJsonObject().get("tags"));
                Metric.add("timestamp", json.getAsJsonObject().get("timestamp"));
                Metric.addProperty("metric", JsonEntry.getKey());
                if (tmpMetric.isJsonPrimitive()) {
                    Metric.add("value", tmpMetric);
                    Metric.addProperty("type", "None");
                    Metric.addProperty("reaction", 0);
                }
                if (tmpMetric.isJsonObject()) {
                    if (tmpMetric.getAsJsonObject().get("value") == null) {
                        baseLOGGER.warn("mtrsc.getValue()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getValue()==null " + msg);
                        continue;
                    } else {
                        Metric.add("value", tmpMetric.getAsJsonObject().get("value"));
                    }
                    if (tmpMetric.getAsJsonObject().get("type") == null) {
                        baseLOGGER.warn("mtrsc.type==null " + Metric);
                        baseLOGGER.warn("mtrsc.type==null " + msg);
                        Metric.addProperty("type", "None");
                    } else {
                        Metric.add("type", tmpMetric.getAsJsonObject().get("type"));
                    }

                    if (tmpMetric.getAsJsonObject().get("reaction") == null) {
                        baseLOGGER.warn("mtrsc.reaction==null " + Metric);
                        baseLOGGER.warn("mtrsc.reaction==null " + msg);
                        Metric.addProperty("reaction", 0);
                    } else {
                        Metric.add("reaction", tmpMetric.getAsJsonObject().get("reaction"));
                    }

                    if ((tmpMetric.getAsJsonObject().get("tags") != null) && (tmpMetric.getAsJsonObject().get("tags").isJsonObject())) {
                        for (Map.Entry<String, JsonElement> tagEntry : tmpMetric.getAsJsonObject().get("tags").getAsJsonObject().entrySet()) {
                            if (tagEntry.getValue().isJsonPrimitive()) {
                                Metric.get("tags").getAsJsonObject().add(tagEntry.getKey(), tagEntry.getValue());
                            }

                        }
                    }

                }

                try {
                    final OddeeyMetric mtrsc = new OddeeyMetric(Metric);
                    if (mtrsc.isSpecial() == special) {
                        if (baseLOGGER.isInfoEnabled()) {
                            if (special) {
                                baseLOGGER.info("Welcom not special tag:" + Metric.toString());
                            } else {
                                baseLOGGER.info("Welcom special tag:" + Metric.toString());
                            }
                        }
                        continue;
                    }

                    if (mtrsc.getName() == null) {
                        baseLOGGER.warn("mtrsc.getName()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getName()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getTimestamp() == null) {
                        baseLOGGER.warn("mtrsc.getTimestamp()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getTimestamp()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getValue() == null) {
                        baseLOGGER.warn("mtrsc.getValue()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getValue()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getTSDBTags() == null) {
                        baseLOGGER.warn("mtrsc.getTSDBTags()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getTSDBTags()==null " + msg);
                        continue;
                    }
                    date = new Date(mtrsc.getTimestamp());
                    baseLOGGER.trace("Time " + date + " Metris: " + mtrsc.getName() + " Host: " + mtrsc.getTags());
                    MetricList.put(mtrsc.hashCode(), mtrsc);

                } catch (Exception e) {
                    baseLOGGER.error("Exception: " + globalFunctions.stackTrace(e));
                    baseLOGGER.error("Exception Wits Metriq: " + Metric);
                    baseLOGGER.info("Exception Wits Input: " + msg);
                }

            }

            if (MetricList.size() > 0) {
                final OddeeyMetric firstmetric = MetricList.firstEntry().getValue();
                baseLOGGER.info(" first metric: Hash " + firstmetric.hashCode() + " list Size: " + MetricList.size() + " Tags hash " + firstmetric.getTags().hashCode() + " Name " + firstmetric.getName() + " Tags " + firstmetric.getTags() + " full json:" + msg);
                return MetricList;
//                Compare.execute(MetricList);
//                        collector.emit(new Values(MetricList));
            }

        }
        return null;
    }

    protected OddeeyMetric prepareJsonObject(JsonElement Metric, String msg) {

        try {
            final OddeeyMetric mtrsc = new OddeeyMetric(Metric);
            if (mtrsc.isSpecial() == special) {
                baseLOGGER.info("Welcom special tag:" + Metric.toString());
                return null;
            }

            if (mtrsc.getName() == null) {
                baseLOGGER.warn("mtrsc.getName()==null " + Metric);
                baseLOGGER.warn("mtrsc.getName()==null " + msg);
                return null;
            }
            if (mtrsc.getTimestamp() == null) {
                baseLOGGER.warn("mtrsc.getTimestamp()==null " + Metric);
                baseLOGGER.warn("mtrsc.getTimestamp()==null " + msg);
                return null;
            }
            if (mtrsc.getValue() == null) {
                baseLOGGER.warn("mtrsc.getValue()==null " + Metric);
                baseLOGGER.warn("mtrsc.getValue()==null " + msg);
                return null;
            }
            if (mtrsc.getTSDBTags() == null) {
                baseLOGGER.warn("mtrsc.getTSDBTags()==null " + Metric);
                baseLOGGER.warn("mtrsc.getTSDBTags()==null " + msg);
                return null;
            }
            date = new Date(mtrsc.getTimestamp());
            baseLOGGER.trace("Time " + date + " Metris: " + mtrsc.getName() + " Host: " + mtrsc.getTags());
            return mtrsc;
//            Compare.execute(mtrsc);
//                    MetricList.put(mtrsc.getName(), mtrsc);
//                            collector.emit(new Values(mtrsc));

        } catch (Exception e) {
            baseLOGGER.error("Exception: " + globalFunctions.stackTrace(e));
            baseLOGGER.error("Exception Wits Metriq: " + Metric);
            baseLOGGER.info("Exception Wits Input: " + msg);
        }
        return null;
    }

    ;
    protected TreeMap<Integer, OddeeyMetric> parseforArray(JsonArray jsonResult, String msg) {
        JsonElement Metric;
        if (jsonResult.size() > 0) {
            baseLOGGER.debug("Ready count: " + jsonResult.size());
            final TreeMap<Integer, OddeeyMetric> MetricList = new TreeMap<>();
            for (int i = 0; i < jsonResult.size(); i++) {
                Metric = jsonResult.get(i);
                try {
                    final OddeeyMetric mtrsc;
                    if (special) {
                        mtrsc = new OddeeysSpecialMetric(Metric);
                    } else {
                        mtrsc = new OddeeyMetric(Metric);
                    }

                    if (mtrsc.isSpecial() != special) {
                        baseLOGGER.info("Welcom special tag:" + Metric.toString());
                        continue;
                    }

                    if (mtrsc.getName() == null) {
                        baseLOGGER.warn("mtrsc.getName()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getName()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getTimestamp() == null) {
                        baseLOGGER.warn("mtrsc.getTimestamp()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getTimestamp()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getValue() == null) {
                        baseLOGGER.warn("mtrsc.getValue()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getValue()==null " + msg);
                        continue;
                    }
                    if (mtrsc.getTSDBTags() == null) {
                        baseLOGGER.warn("mtrsc.getTSDBTags()==null " + Metric);
                        baseLOGGER.warn("mtrsc.getTSDBTags()==null " + msg);
                        continue;
                    }
                    date = new Date(mtrsc.getTimestamp());
                    baseLOGGER.trace("Time " + date + " Metris: " + mtrsc.getName() + " Host: " + mtrsc.getTags());
                    MetricList.put(mtrsc.hashCode(), mtrsc);

                } catch (Exception e) {
                    baseLOGGER.error("Exception: " + globalFunctions.stackTrace(e));
                    baseLOGGER.error("Exception Wits Metriq: " + Metric);
                    baseLOGGER.info("Exception Wits Input: " + msg);
                }

            }

            if (MetricList.size() > 0) {
                final OddeeyMetric firstmetric = MetricList.firstEntry().getValue();
                baseLOGGER.info(" first metric: Hash " + firstmetric.hashCode() + " list Size: " + MetricList.size() + " Tags hash " + firstmetric.getTags().hashCode() + " Name " + firstmetric.getName() + " Tags " + firstmetric.getTags() + " full json:" + msg);
//                Compare.execute(MetricList);
                return MetricList;
//                        collector.emit(new Values(MetricList));
            }

        }
        return null;
    }

    @Override
    public void execute(Tuple input) {
        String msg = input.getString(0);
        baseLOGGER.debug("Start messge:" + msg);

        JsonElement jsonResult = null;
        try {
            jsonResult = this.parser.parse(msg);
        } catch (JsonSyntaxException ex) {
            baseLOGGER.info("msg parse Exception" + ex.toString());
        }
        Object output = null;
        if (jsonResult != null) {
            try {
                if (this.parser.parse(msg).isJsonArray()) {
                    output = this.parseforArray(jsonResult.getAsJsonArray(), msg);
                } else {
                    int version = 1;
                    if (jsonResult.getAsJsonObject().get("version") != null) {
                        version = jsonResult.getAsJsonObject().get("version").getAsInt();
                    }
                    if (version == 1) {
                        output = this.prepareJsonObject(jsonResult, msg);
                    }
                    if (version == 2) {
                        output = this.prepareJsonObjectV2(jsonResult, msg);
                    }
                }

            } catch (JsonSyntaxException ex) {
                baseLOGGER.error("JsonSyntaxException: " + globalFunctions.stackTrace(ex));
            } catch (NumberFormatException ex) {
                baseLOGGER.error("NumberFormatException: " + globalFunctions.stackTrace(ex));

            }
        }
        if (output != null) {
//            if (special) {
//                CompareSpec.execute(output);
//            } else {
//                Compare.execute(output);
//            }

            collector.emit(new Values(output));
        }
        this.collector.ack(input);
    }
}
