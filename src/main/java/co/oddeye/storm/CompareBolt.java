/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.AlertLevel;
import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.google.gson.JsonObject;
import java.util.logging.Level;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CompareBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(CompareBolt.class);
    private final java.util.Map<String, Object> conf;
    private org.hbase.async.Config clientconf;
    private Config openTsdbConfig;
    private OddeeyMetricMetaList mtrscList;
    private final byte[] meta_family = "d".getBytes();
    private final byte[] metatable;
    private byte[] key;
    private Calendar CalendarObjRules;
    private final Calendar CalendarObj;

    private int weight;
    private int curent_DW;
    private int local_DW;
    private int weight_KF;
//    private int weight_D_KF;
    private final int devkef = 1;

    private byte[] errortable;
    private final byte[] error_family = "d".getBytes();
//    private OddeeyMetricMeta oldmtrc;
    private double tmp_weight_per;
    private int loop;
    private double weight_per;
    private JsonParser parser = null;
    private JsonObject jsonResult = null;

    /**
     *
     * @param config
     */
    public CompareBolt(java.util.Map config) {
        this.conf = config;
        this.metatable = String.valueOf(this.conf.get("metatable")).getBytes();
        CalendarObjRules = Calendar.getInstance();
        CalendarObj = Calendar.getInstance();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("mtrsc", "metric"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        LOGGER.warn("DoPrepare WriteToTSDBseries");
        collector = oc;
        parser = new JsonParser();
        try {
            errortable = String.valueOf(conf.get("errorstable")).getBytes();
            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");
            globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf);

            try {
                LOGGER.warn("Start read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf), this.metatable);
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

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceComponent().equals("kafkaSemaphoreSpot")) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("message from kafkaSemaphoreSpot" + tuple.getString(0));
            }
            collector.ack(tuple);
            jsonResult = this.parser.parse(tuple.getString(0)).getAsJsonObject();
            if (jsonResult.get("action").getAsString().equals("resetregresion")) {
                final String uuid = jsonResult.get("UUID").getAsString();
                final int hash = jsonResult.get("hash").getAsInt();
                if (mtrscList.containsKey(hash)) {
                    try {
                        final OddeeyMetricMeta mtrsc = mtrscList.get(hash);
                        mtrsc.getRegression().clear();

                        byte[] qualifier = "Regression".getBytes();
                        byte[] value = mtrsc.getSerializedRegression();
                        PutRequest putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifier, value);
                        globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                        mtrscList.set(mtrsc);
                    } catch (IOException ex) {
                        java.util.logging.Logger.getLogger(CompareBolt.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }

        if (tuple.getSourceComponent().equals("ParseMetricBolt")) {
            try {
                final OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
                final OddeeyMetricMeta mtrscinput = new OddeeyMetricMeta(metric, globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf));
                PutRequest putvalue;
                key = mtrscinput.getKey();
                byte[][] qualifiers;
                byte[][] values;

                collector.ack(tuple);
                if (mtrscList == null) {
                    LOGGER.error("Es anasunucjun@ vonca null darel");
                    try {
                        LOGGER.warn("Start read meta in hbase");
                        mtrscList = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable);
                        LOGGER.warn("End read meta in hbase");
                    } catch (Exception ex) {
                        mtrscList = new OddeeyMetricMetaList();
                    }
                }
                Integer code = 0;
                try {
                    code = mtrscinput.hashCode();
                } catch (Exception ex) {
                    LOGGER.error("In hashCode: " + metric.getName() + " " + globalFunctions.stackTrace(ex));
                }
                final OddeeyMetricMeta mtrsc;
                if (code != 0) {
                    if (!mtrscList.containsKey(code)) {
                        mtrsc = mtrscinput;
                        GetRequest getRegression = new GetRequest(metatable, key, meta_family, "Regression".getBytes());
                        ArrayList<KeyValue> Regressiondata = globalFunctions.getSecindaryclient(clientconf).get(getRegression).joinUninterruptibly();
                        for (KeyValue Regression : Regressiondata) {
                            if (Arrays.equals(Regression.qualifier(), "Regression".getBytes())) {
                                mtrsc.setSerializedRegression(Regression.value());
                            }
                        }
                        mtrsc.getRegression().addData(metric.getTimestamp(), metric.getValue());
                        qualifiers = new byte[4][];
                        values = new byte[4][];
                        qualifiers[0] = "n".getBytes();
                        qualifiers[1] = "timestamp".getBytes();
                        qualifiers[2] = "Regression".getBytes();
                        qualifiers[3] = "type".getBytes();
                        values[0] = key;
                        values[1] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                        values[2] = mtrsc.getSerializedRegression();
                        values[3] = ByteBuffer.allocate(2).putShort(metric.getType()).array(); 
                        putvalue = new PutRequest(metatable, key, meta_family, qualifiers, values);
                        LOGGER.info("Add metric Meta to hbase:" + mtrsc.getName() + " tags " + mtrsc.getTags());
                    } else {
//                        oldmtrc = mtrsc;
                        mtrsc = mtrscList.get(mtrscinput.hashCode());
                        mtrsc.getRegression().addData(metric.getTimestamp(), metric.getValue());
                        if (!Arrays.equals(mtrsc.getKey(), key)) {
                            LOGGER.warn("More key for single hash:" + mtrsc.getName() + " tags " + mtrsc.getTags() + "More key for single hash:" + mtrscinput.getName() + " tags " + mtrscinput.getTags() + " mtrsc.getKey() = " + Hex.encodeHexString(mtrsc.getKey()) + " Key= " + Hex.encodeHexString(key));
                        }

                        qualifiers = new byte[2][];
                        values = new byte[2][];

                        qualifiers[0] = "timestamp".getBytes();
                        qualifiers[1] = "Regression".getBytes();
                        values[0] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                        values[1] = mtrsc.getSerializedRegression();
                        putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifiers, values);
                        LOGGER.info("Update timastamp:" + mtrsc.getName() + " tags " + mtrsc.getTags() + " Stamp " + metric.getTimestamp());
                    }
                    globalFunctions.getSecindaryclient(clientconf).put(putvalue);

                    if (!metric.getName().equals("host_absent")) {

                        CalendarObj.setTimeInMillis(metric.getTimestamp());
                        CalendarObjRules.setTimeInMillis(metric.getTimestamp());
                        CalendarObjRules.add(Calendar.DATE, -1);
                        final Map<String, MetriccheckRule> Rules = mtrsc.getRules(CalendarObjRules, 7, metatable, globalFunctions.getSecindaryclient(clientconf));
                        final int alert_level = metric.getReaction();
                        short input_weight = 0;
                        if (alert_level>0) {
                            input_weight = (short) alert_level;
                        }
                        weight_per = 0;
                        loop = 0;
                        weight = 0;
                        if ((input_weight < 1) && (input_weight > -3)) {
                            curent_DW = CalendarObj.get(Calendar.DAY_OF_WEEK);
                            LOGGER.info(CalendarObj.getTime() + "-" + metric.getName() + " " + metric.getTags().get("host"));
                            for (Map.Entry<String, MetriccheckRule> RuleEntry : Rules.entrySet()) {
                                loop++;
                                final MetriccheckRule Rule = RuleEntry.getValue();
                                if (Rule == null) {
                                    LOGGER.warn("Rule is NUll: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                    continue;
                                }

                                CalendarObjRules = MetriccheckRule.QualifierToCalendar(Rule.getQualifier());

                                if (!Rule.isIsValidRule()) {
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("No rule for check in cache: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                    }
                                    continue;
                                }
                                if (Rule.isHasNotData()) {
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("rule Has no data for check in cache: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                    }
                                    continue;
                                }
                                local_DW = CalendarObjRules.get(Calendar.DAY_OF_WEEK);
                                if (curent_DW == local_DW) {
                                    weight_KF = 2;

                                } else {
                                    weight_KF = 1;
                                }
                                if (Rule.getAvg() != null) {

                                    if ((Rule.getAvg() != 0) && (metric.getValue() != 0)) {
                                        tmp_weight_per = (metric.getValue() - Rule.getAvg()) / Rule.getAvg() * 100;
                                    } else {
                                        if (metric.getValue() == 0) {
                                        }
                                        tmp_weight_per = 0;
                                    }
                                }

                                if (input_weight != -1) {
                                    if (Rule.getAvg() != null && Rule.getDev() != null) {
                                        if (metric.getValue() > Rule.getAvg() + devkef * Rule.getDev()) {
                                            weight = (short) (weight + weight_KF);
                                            weight_per = weight_per + tmp_weight_per;
                                        }
                                    }
                                    if (Rule.getMax() != null) {
                                        if (metric.getValue() > Rule.getMax()) {
                                            weight = (short) (weight + weight_KF);
                                        }
                                    }
                                } else {
                                    if (LOGGER.isInfoEnabled()) {
                                        LOGGER.info("Check Up Disabled : Withs weight" + input_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                    }
                                }

                                if (input_weight != -2) {
                                    if (Rule.getMin() != null) {
                                        if (metric.getValue() < Rule.getMin()) {
                                            weight = (short) (weight - weight_KF);
                                            weight_per = weight_per + tmp_weight_per;
                                        }
                                    }
                                    if (Rule.getAvg() != null && Rule.getDev() != null) {
                                        if (metric.getValue() < Rule.getAvg() - devkef * Rule.getDev()) {
                                            weight = (short) (weight - weight_KF);
                                        }
                                    }
                                } else {
                                    LOGGER.warn("Check Down Disabled : Withs weight" + input_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                                }
                            }
                        } else if (input_weight > 0) {
                            if (metric.getValue() > input_weight) {
                                weight = 16;
                            } else {
                                weight = 0;
                            }
                        } else if (input_weight == -4) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Check disabled by so old messge: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            }
                        } else if (input_weight == -5) {
                            LOGGER.warn("Check disabled by Topology: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                        } else {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Check disabled by user: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            }
                        }

                        if (weight != 0) {
                            final AlertLevel AlertLevel = new AlertLevel();

                            weight_per = weight_per / loop;
                            double predict_value = mtrsc.getRegression().predict(CalendarObj.getTimeInMillis());
                            double predict_value_per = 0;
                            if ((!Double.isNaN(predict_value)) && (predict_value != 0)) {
                                predict_value_per = (metric.getValue() - predict_value) / predict_value * 100;
                            }
                            // TODO Karoxa hanel aradzin bolt
                            key = mtrsc.getTags().get("UUID").getValueTSDBUID();
                            key = ArrayUtils.addAll(key, ByteBuffer.allocate(8).putLong((long) (CalendarObj.getTimeInMillis() / 1000)).array());

                            putvalue = new PutRequest(errortable, key, error_family, mtrsc.getKey(), ByteBuffer.allocate(26).putShort((short) weight).putDouble(weight_per).putDouble(metric.getValue()).putDouble(predict_value_per).array());
                            mtrsc.getLevelList().add(AlertLevel.getErrorLevel(weight, weight_per, metric.getValue(), predict_value_per));
                            globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("Put Error" + weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                            }
                        } else {
                            mtrsc.getLevelList().add(-1);
                        }
                        if (mtrsc.getLevelList().size() > 10) {
                            mtrsc.getLevelList().remove(0);
                        }

                        if (Collections.max(mtrsc.getLevelList()) > -1) {
                            Map<Integer, Integer> Errormap = new TreeMap<>(Collections.reverseOrder());
                            for (Integer e : mtrsc.getLevelList()) {
                                if (e > -1) {
                                    for (int j = e; j >= 0; j--) {
                                        Integer counter = Errormap.get(j);
                                        if (counter != null) {
                                            counter++;
                                        } else {
                                            counter = 1;
                                        }

                                        Errormap.put(j, counter);
                                    }

                                }
                            }
                            if (!Errormap.isEmpty()) {
                                boolean setlevel = false;
                                for (Map.Entry<Integer, Integer> item : Errormap.entrySet()) {
                                    if (item.getValue() > 4) {
                                        mtrsc.getErrorState().setLevel(item.getKey(), metric.getTimestamp());
                                        setlevel = true;
                                        break;
                                    }

                                }
                                if (!setlevel) {
                                    mtrsc.getErrorState().setLevel(mtrsc.getErrorState().getLevel(), metric.getTimestamp());
                                }

                            }
                            CalendarObj.setTimeInMillis(metric.getTimestamp());
                            if (mtrsc.getErrorState().getState() > -1) {
                                collector.emit(new Values(mtrsc, metric));
                            }
                        } else {
                            if (mtrsc.getErrorState().getLevel() != -1) {
                                mtrsc.getErrorState().setLevel(-1, metric.getTimestamp());
                                collector.emit(new Values(mtrsc, metric));
                            }
                        }
                        mtrscList.set(mtrsc);
                    }
                }
            } catch (RuntimeException ex) {
                LOGGER.error("RuntimeException In big try:" + globalFunctions.stackTrace(ex) + tuple.getValueByField("metric"));
            } catch (Exception ex) {
                LOGGER.error("In big try:" + globalFunctions.stackTrace(ex) + tuple.getValueByField("metric"));

            }
        }
    }
}
