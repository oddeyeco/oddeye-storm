/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

//import com.fasterxml.jackson.databind.ObjectMapper;
import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.hbase.async.PutRequest;
//import net.spy.memcached.MemcachedClient;

/**
 *
 * @author vahan
 */
public class KafkaOddeyeMsgToTSDBBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    private static TSDB tsdb = null;

    private JsonParser parser = null;
    private JsonArray jsonResult = null;
    private final java.util.Map<String, Object> conf;
    private static org.hbase.async.HBaseClient client;

    private byte[] metatable;
    private short p_weight;
    private int weight;
    private byte[] key;
    private org.hbase.async.Config clientconf;
    private Config openTsdbConfig;
    private int mb;
    private OddeeyMetricMeta mtrsc;
    private OddeeyMetricMetaList mtrscList;
    private final byte[] meta_family = "d".getBytes();

    private double d_value;
    private JsonElement alert_level;
    private Calendar CalendarObjRules;
    private MetriccheckRule Rule;
    private long metrictime;
    private Calendar CalendarObj;
    private Boolean DisableCheck;
    private int curent_DW;
    private int local_DW;
    private int weight_KF;
    private int weight_D_KF;

    /**
     *
     * @param config
     */
    public KafkaOddeyeMsgToTSDBBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void execute(Tuple input) {
        CalendarObj = Calendar.getInstance();
        CalendarObjRules = Calendar.getInstance();
        String msg = input.getString(0);
        this.collector.ack(input);
        LOGGER.debug("Start KafkaOddeyeMsgToTSDBBolt " + msg);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        java.util.Date date = new java.util.Date();
        LOGGER.info("Bolt ready to write to TSDB " + df.format(date.getTime()));
        JsonElement Metric;
        try {
            this.jsonResult = (JsonArray) this.parser.parse(msg);
        } catch (Exception ex) {
            LOGGER.info("msg parse Exception" + ex.toString());
        }
        HashMap<String, String> tags = new HashMap<>();
        while (KafkaOddeyeMsgToTSDBBolt.tsdb == null) {
            try {
                KafkaOddeyeMsgToTSDBBolt.client = new org.hbase.async.HBaseClient(clientconf);
                KafkaOddeyeMsgToTSDBBolt.tsdb = new TSDB(
                        KafkaOddeyeMsgToTSDBBolt.client,
                        openTsdbConfig);
            } catch (Exception e) {
                LOGGER.warn("OpenTSDB Connection fail in run");
                LOGGER.error("Exception: " + stackTrace(e));
            }

        }

        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    LOGGER.debug("Ready count: " + this.jsonResult.size());
                    Metric = this.jsonResult.get(0);
                    try {
                        metrictime = Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000;
                        CalendarObj.setTimeInMillis(metrictime);
                        CalendarObjRules.setTime(new Date());
                        CalendarObjRules.add(Calendar.HOUR, -1);
                        LOGGER.info("Messge Time: " + CalendarObj.getTime().toString());
                    } catch (Exception e) {
                        LOGGER.error("Exception: " + stackTrace(e));
                        LOGGER.error("Wits First Metric: " + Metric);
                        LOGGER.error("Wits Full message: " + msg);
//                        this.collector.ack(input);
                    }

                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        try {
                            metrictime = Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000;
                            CalendarObj.setTimeInMillis(metrictime);
                            mtrsc = new OddeeyMetricMeta(Metric, tsdb);
                            if (!mtrscList.containsKey(mtrsc.hashCode())) {
                                key = mtrsc.getKey();
                                PutRequest putvalue = new PutRequest(metatable, key, meta_family, "n".getBytes(), key);
                                client.put(putvalue);
                                LOGGER.warn("Add metric Meta to hbase:" + mtrsc.getName() + " tags " + mtrsc.getTags());
                            } else {
                                mtrsc = mtrscList.get(mtrsc.hashCode());
                            }
                            d_value = Metric.getAsJsonObject().get("value").getAsDouble();
                            alert_level = Metric.getAsJsonObject().get("tags").getAsJsonObject().get("alert_level");
                            p_weight = 0;
                            if (null != alert_level) {
                                p_weight = Short.parseShort(alert_level.getAsString());
                            }
                            if (CalendarObjRules.getTimeInMillis() > CalendarObj.getTimeInMillis()) {
                                p_weight = -4;
//                                this.collector.ack(input);
                            }
                            if (DisableCheck) {
                                p_weight = -5;
//                                this.collector.ack(input);
                            }

                                if ((alert_level == null) || ((p_weight < 1) && (p_weight > -3))) {
                                    weight = 0;
                                    CalendarObjRules.setTimeInMillis(metrictime);
                                    curent_DW = CalendarObjRules.get(Calendar.DAY_OF_WEEK);
                                    LOGGER.info(CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                    for (int j = 0; j < 7; j++) {
                                        CalendarObjRules.add(Calendar.DATE, -1);
                                        try {
                                            Rule = mtrsc.getRule(CalendarObjRules, metatable, client);
                                        } catch (Exception ex) {
                                            LOGGER.warn("Rule exeption: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                            LOGGER.warn("RuleExeption: " + stackTrace(ex));
                                        }
                                        if (Rule == null) {
                                            LOGGER.warn("Rule is NUll: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                            continue;
                                        }

                                        if (!Rule.isIsValidRule()) {
                                            LOGGER.info("No rule for check in cache: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                            continue;
                                        }
//                                        LOGGER.warn("Rule: " + Rule);
                                        local_DW = CalendarObjRules.get(Calendar.DAY_OF_WEEK);
                                        if (curent_DW == local_DW) {
                                            weight_KF = 2;
                                            weight_D_KF = -1;
                                        } else {
                                            weight_KF = 1;
                                            weight_D_KF = 0;
                                        }
                                        if (p_weight != -1) {
                                            if (Rule.getAvg() != null && Rule.getDev() != null) {
                                                if (d_value > Rule.getAvg() + Rule.getDev()) {
                                                    weight = (short) (weight + weight_KF);
                                                }
                                            }
                                            if (Rule.getMax() != null) {
                                                if (d_value > Rule.getMax()) {
                                                    weight = (short) (weight + weight_KF);
                                                }
                                            }
                                        } else {
                                            LOGGER.info("Check Up Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                        }

                                        if (p_weight != -2) {
                                            if (Rule.getMin() != null) {
                                                if (d_value < Rule.getMin()) {
                                                    weight = (short) (weight + weight_KF);
                                                }
                                            }
                                            if (Rule.getAvg() != null && Rule.getDev() != null) {
                                                if (d_value < Rule.getAvg() - Rule.getDev()) {
                                                    weight = (short) (weight + weight_KF);
                                                }
                                            }

                                            if ((d_value < Rule.getAvg() + Rule.getDev()) && (d_value > Rule.getAvg() - Rule.getDev())) {
                                                weight = (short) (weight + weight_D_KF);
                                            }
                                            if ((d_value < Rule.getMax()) && (d_value > Rule.getMin())) {
                                                weight = (short) (weight + weight_D_KF);
                                            }

                                        } else {
                                            LOGGER.info("Check Down Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                        }
                                    }
                                    if (weight < 0) {
                                        weight = 0;
                                    }
                                    p_weight = (short) weight;
                                }
                                else if (p_weight > 0) {
                                if (d_value > p_weight) {
                                    p_weight = 16;
                                }
                                else
                                {
                                   p_weight = 0; 
                                }
                            } else if (p_weight == -4) {
                                LOGGER.info("Check disabled by so old messge: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            } else if (p_weight == -5) {
                                LOGGER.warn("Check disabled by Topology: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            } else {
                                LOGGER.info("Check disabled by user: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            }
                            tags.clear();
                            mtrsc.getTags().entrySet().stream().forEach((tag) -> {
                                tags.put(tag.getKey(), tag.getValue().getValue());
                            });
//                            tags.put("alert_level", Short.toString(p_weight));

                            try {
                                tsdb.addPoint(mtrsc.getName(), CalendarObj.getTimeInMillis(), d_value, tags);
                            } catch (IllegalArgumentException e) {
                                LOGGER.error("Exception: " + stackTrace(e));
                                LOGGER.error("Add point Wits Metric: " + Metric);
                                LOGGER.error("Add point Wits Input: " + msg);
                            }

                            mtrscList.set(mtrsc);
                            if (p_weight > 0) {
                                try {
                                    LOGGER.info("Emit error metric " + p_weight + " to date " + CalendarObj.getTime() + " For metric:" + mtrsc.getName());
                                    this.collector.emit(new Values(mtrsc, p_weight, CalendarObj));
                                    LOGGER.info("End Put error " + p_weight + " to date " + CalendarObj.getTime() + " For metric:" + mtrsc.getName());
                                } catch (Exception e) {
                                    LOGGER.error("Error Emit :" + mtrsc.getName() + " to date " + CalendarObj.getTime() + " tags " + mtrsc.getTags());
                                    LOGGER.error("JsonSyntaxException: " + stackTrace(e));
                                }

                            }

                            LOGGER.debug("Add metric Value:" + mtrsc.getName());

                        } catch (Exception e) {
                            LOGGER.error("Exception: " + stackTrace(e));
                            LOGGER.error("Exception Wits Metriq: " + Metric);
                            LOGGER.error("Exception Wits Input: " + msg);
                        }

                    }
//                    this.collector.ack(input);
                    LOGGER.debug("metric cache size:" + mtrscList.size());

                }
            } catch (JsonSyntaxException ex) {
                LOGGER.error("JsonSyntaxException: " + stackTrace(ex));
//                this.collector.ack(input);
            } catch (NumberFormatException ex) {
                LOGGER.error("NumberFormatException: " + stackTrace(ex));
//                this.collector.ack(input);
            }
            this.jsonResult = null;
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metric", "weight", "calendar"));
    }

    @Override
    public void cleanup() {
        try {
            this.tsdb.shutdown().joinUninterruptibly();
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB shutdown execption : " + ex.toString());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector i_collector) {
        LOGGER.warn("DoPrepare KafkaOddeyeMsgToTSDBBolt");
        collector = i_collector;
        parser = new JsonParser();
        mb = 1024 * 1024;

        //Getting the runtime reference from system
//        runtime = Runtime.getRuntime();
        try {
//            t_cache = new MemcachedClient(
//                    new InetSocketAddress("192.168.10.60", 11211));
            //TODO do config

            DisableCheck = Boolean.valueOf(String.valueOf(conf.get("DisableCheck")));

            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");

            while (KafkaOddeyeMsgToTSDBBolt.tsdb == null) {
                try {
                    KafkaOddeyeMsgToTSDBBolt.client = new org.hbase.async.HBaseClient(clientconf);
                    KafkaOddeyeMsgToTSDBBolt.tsdb = new TSDB(
                            KafkaOddeyeMsgToTSDBBolt.client,
                            openTsdbConfig);
                } catch (Exception e) {
                    LOGGER.warn("OpenTSDB Connection fail in prepare");
                    LOGGER.error("Exception: " + stackTrace(e));
                }

            }

            try {
                LOGGER.warn("Start read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(tsdb, this.metatable);
                LOGGER.warn("End read meta in hbase");
            } catch (Exception ex) {
                mtrscList = new OddeeyMetricMetaList();
            }

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");

    }
}
