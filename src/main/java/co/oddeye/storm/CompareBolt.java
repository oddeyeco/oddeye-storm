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
import com.google.common.collect.Iterables;
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
import java.util.HashMap;
import java.util.TimeZone;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
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
    private OddeeyMetricMetaList MetricMetaList;
    private Calendar CalendarObjRules;
    private final Calendar CalendarObj;

    private final int devkef = 1;

    private final byte[] metatable;
    private final byte[] meta_family = "d".getBytes();

    private final byte[] usertable;
    private final byte[] usertechfamily = "technicalinfo".getBytes();

    private final byte[] errortable;
    private final byte[] error_family = "d".getBytes();

    private JsonParser parser = null;
    private JsonObject jsonResult = null;

    private final Map<String, AlertLevel> UserLevels = new HashMap<>();

    /**
     *
     * @param config
     */
    public CompareBolt(java.util.Map config) {
        this.conf = config;
        this.metatable = String.valueOf(this.conf.get("metatable")).getBytes();
        this.usertable = String.valueOf(this.conf.get("usertable")).getBytes();
        this.errortable = String.valueOf(conf.get("errorstable")).getBytes();
        CalendarObjRules = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        CalendarObj = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
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
            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", String.valueOf(conf.get("hbase.rpcs.batch.size")));
        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        }
        try {
            globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf);
            try {
                LOGGER.warn("Start read meta in hbase");
                MetricMetaList = new OddeeyMetricMetaList(globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf), this.metatable);
                LOGGER.warn("End read meta in hbase");
            } catch (Exception ex) {
                MetricMetaList = new OddeeyMetricMetaList();
            }

            try {
                Scanner scanner = globalFunctions.getSecindaryclient(clientconf).newScanner(usertable);
                scanner.setServerBlockCache(false);
                scanner.setMaxNumRows(1000);
                scanner.setFamily(usertechfamily);
//            scanner.setQualifier("n".getBytes());
                final byte[][] Qualifiers = new byte[][]{"AL".getBytes()};
                scanner.setQualifiers(Qualifiers);

                ArrayList<ArrayList<KeyValue>> rows;
                while ((rows = scanner.nextRows(1000).joinUninterruptibly()) != null) {

                    for (final ArrayList<KeyValue> row : rows) {
                        String ALjson = "";
//                    String UsID = "";
                        String UsID = new String(row.get(0).key());
                        for (KeyValue cell : row) {
                            if (Arrays.equals(cell.qualifier(), "AL".getBytes())) {
                                ALjson = new String(cell.value());
                            }
                        }
                        if (!UsID.isEmpty()) {
                            final AlertLevel AlertLevels;
                            if (!ALjson.isEmpty()) {
                                AlertLevels = globalFunctions.getGson().fromJson(ALjson, AlertLevel.class);
                            } else {
                                AlertLevels = new AlertLevel(true);
                                LOGGER.warn("Add UserLevels list : " + UsID + ": EMPTY");
                            }
                            UserLevels.put(UsID, AlertLevels);                            
                        }

                    }

                }
                
            } catch (Exception e) {
                LOGGER.error("OpenTSDB config execption : " + e.toString());
            }

        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceComponent().equals("SemaforProxyBolt")) {

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("message from SemaforProxyBolt" + tuple.getValueByField("action").toString());
            }

            jsonResult = this.parser.parse(tuple.getValueByField("action").toString()).getAsJsonObject();

            if (jsonResult.get("action").getAsString().equals("deletemetricbyhash")) {
                final int hash = jsonResult.get("hash").getAsInt();
                if (MetricMetaList.containsKey(hash)) {
                    MetricMetaList.remove(hash);
                }
            }

            if (jsonResult.get("action").getAsString().equals("resetregresion")) {
                final int hash = jsonResult.get("hash").getAsInt();
                if (MetricMetaList.containsKey(hash)) {
                    try {
                        final OddeeyMetricMeta mtrsc = MetricMetaList.get(hash);
                        mtrsc.getRegression().clear();

                        byte[] qualifier = "Regression".getBytes();
                        byte[] value = mtrsc.getSerializedRegression();
                        PutRequest putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifier, value);
                        globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                        MetricMetaList.set(mtrsc);
                    } catch (IOException ex) {
                        LOGGER.error(globalFunctions.stackTrace(ex));
                    }
                }
            }
            if (jsonResult.get("action").getAsString().equals("updatelevels")) {
                final String uuid = jsonResult.get("UUID").getAsString();
                final String changedata = jsonResult.get("changedata").getAsString();
                AlertLevel AlertLevels = null;
                try {
                    AlertLevels = globalFunctions.getGson().fromJson(changedata, AlertLevel.class);
                } finally {
                    if (AlertLevels != null) {
                        UserLevels.put(uuid, AlertLevels);
                    }
                }
                LOGGER.warn("Edit UserLevels list Semaphore : " + uuid + ":" + changedata);
                
            }

        }

        if (tuple.getSourceComponent().equals("ParseMetricBolt")) {
            if (tuple.getValueByField("MetricField") instanceof Map) {
                Map<String, OddeeyMetric> MetricList = (Map<String, OddeeyMetric>) tuple.getValueByField("MetricField");
                for (Map.Entry<String, OddeeyMetric> metricEntry : MetricList.entrySet()) {
                    OddeeyMetric metric = metricEntry.getValue();
                    this.checkMetric(metric);
                }
            }

            if (tuple.getValueByField("MetricField") instanceof OddeeyMetric) {
                this.checkMetric((OddeeyMetric) tuple.getValueByField("MetricField"));
            }
        }
        this.collector.ack(tuple);
    }

    public void checkMetric(OddeeyMetric metric) {
        try {
            OddeeyMetricMeta mtrscMetaInput = new OddeeyMetricMeta(metric, globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf));
            OddeeyMetricMeta mtrscMetaLocal;

            PutRequest putvalue;
            byte[] key = mtrscMetaInput.getKey();
            byte[][] qualifiers;
            byte[][] values;
            Integer code = 0;
            try {
                code = mtrscMetaInput.hashCode();
            } catch (Exception ex) {
                LOGGER.error("In hashCode: " + metric.getName() + " " + globalFunctions.stackTrace(ex));
            }

            if (code != 0) {
                if (!MetricMetaList.containsKey(code)) {
                    mtrscMetaLocal = mtrscMetaInput;
                    mtrscMetaLocal.getRegression().addData(metric.getTimestamp() / 1000, metric.getValue());
                    qualifiers = new byte[4][];
                    values = new byte[4][];
                    qualifiers[0] = "n".getBytes();
                    qualifiers[1] = "timestamp".getBytes();
                    qualifiers[2] = "Regression".getBytes();
                    qualifiers[3] = "type".getBytes();
                    values[0] = key;
                    values[1] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                    try {
                        values[2] = mtrscMetaLocal.getSerializedRegression();
                    } catch (IOException ex) {
                        LOGGER.error("In getSerializedRegression: " + metric.getName() + " " + globalFunctions.stackTrace(ex));
                        values[2] = new byte[0];
                    }
                    values[3] = ByteBuffer.allocate(2).putShort(metric.getType()).array();
                    putvalue = new PutRequest(metatable, key, meta_family, qualifiers, values);
                    LOGGER.info("Add metric Meta to hbase:" + mtrscMetaLocal.getName() + " tags " + mtrscMetaLocal.getTags() + " code " + code + " newcode: " + mtrscMetaLocal.hashCode());
                    globalFunctions.getSecindaryclient(clientconf).put(putvalue).joinUninterruptibly();
//                    globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                } else {
                    mtrscMetaLocal = MetricMetaList.get(mtrscMetaInput.hashCode());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("metric interval: " + mtrscMetaInput.hashCode() + " " + mtrscMetaInput.getName() + " mtrscMetaInput.getLasttime: " + mtrscMetaInput.getLasttime() + " mtrscMetaLocal.getLasttime():" + mtrscMetaLocal.getLasttime() + " " + (mtrscMetaInput.getLasttime() - mtrscMetaLocal.getLasttime()));
                    }
                    if ((mtrscMetaInput.getLasttime() <= mtrscMetaLocal.getLasttime())) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Metric Negativ interval: " + mtrscMetaInput.hashCode() + " " + mtrscMetaInput.getName() + " " + mtrscMetaInput.getLasttime() + " " + (mtrscMetaInput.getLasttime() - mtrscMetaLocal.getLasttime()));
                        }

                        return;
                    }
                    mtrscMetaLocal.getRegression().addData(metric.getTimestamp() / 1000, metric.getValue());

                    if (!Arrays.equals(mtrscMetaLocal.getKey(), key)) {
                        LOGGER.warn("More key for single hash:" + mtrscMetaLocal.getName() + " tags " + mtrscMetaLocal.getTags() + "More key for single hash:" + mtrscMetaInput.getName() + " tags " + mtrscMetaInput.getTags() + " mtrsc.getKey() = " + Hex.encodeHexString(mtrscMetaInput.getKey()) + " Key= " + Hex.encodeHexString(key));
                    }
                    qualifiers = new byte[2][];
                    values = new byte[2][];
                    if (mtrscMetaInput.getType() != mtrscMetaLocal.getType()) {
                        qualifiers = new byte[3][];
                        values = new byte[3][];
                        qualifiers[2] = "type".getBytes();
                        values[2] = ByteBuffer.allocate(2).putShort(mtrscMetaInput.getType()).array();
                        mtrscMetaLocal.setType(mtrscMetaInput.getType());
                    }

                    qualifiers[0] = "timestamp".getBytes();
                    qualifiers[1] = "Regression".getBytes();
                    values[0] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                    values[1] = mtrscMetaLocal.getSerializedRegression();
                    putvalue = new PutRequest(metatable, mtrscMetaLocal.getKey(), meta_family, qualifiers, values);
                    LOGGER.info("Update timastamp:" + mtrscMetaLocal.getName() + " tags " + mtrscMetaLocal.getTags() + " Stamp " + metric.getTimestamp());
                    globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                    mtrscMetaLocal.setLasttime(mtrscMetaInput.getLasttime());
                }
//                        System.out.println(mtrscMetaLocal.getErrorState().getLevel()); 

                MetricMetaList.set(mtrscMetaLocal);

                CalendarObj.setTimeInMillis(metric.getTimestamp());
                CalendarObjRules.setTimeInMillis(metric.getTimestamp());
                CalendarObjRules.add(Calendar.DATE, -1);
                final Map<String, MetriccheckRule> Rules = mtrscMetaLocal.getRules(CalendarObjRules, 7, metatable, globalFunctions.getSecindaryclient(clientconf));
                final int reaction = metric.getReaction();
                short input_weight = (short) reaction;
                double weight_per = 0;
                int loop = 0;
                int weight = 0;

                if ((input_weight < 1) && (input_weight > -3)) {
                    int curent_DW = CalendarObj.get(Calendar.DAY_OF_WEEK);
                    LOGGER.info(CalendarObj.getTime() + "-" + metric.getName() + " " + metric.getTags());
                    double tmp_weight_per = 0;
//                    tmp_weight_per = 0;
                    for (Map.Entry<String, MetriccheckRule> RuleEntry : Rules.entrySet()) {
                        loop++;
                        final MetriccheckRule Rule = RuleEntry.getValue();
                        if (Rule == null) {
                            LOGGER.warn("Rule is NUll: " + CalendarObjRules.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                            continue;
                        }

                        CalendarObjRules = MetriccheckRule.QualifierToCalendar(Rule.getQualifier());

                        if (!Rule.isIsValidRule()) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("No rule for check in cache: " + CalendarObjRules.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                            }
                            continue;
                        }
                        if (Rule.isHasNotData()) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("rule Has no data for check in cache: " + CalendarObjRules.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                            }
                            continue;
                        }
                        int local_DW = CalendarObjRules.get(Calendar.DAY_OF_WEEK);
                        int weight_KF;

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
                                LOGGER.info("Check Up Disabled : Withs weight" + input_weight + " " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
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
                            LOGGER.warn("Check Down Disabled : Withs weight" + input_weight + " " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
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
                        LOGGER.info("Check disabled by so old messge: " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                    }
                } else if (input_weight == -5) {
                    LOGGER.warn("Check disabled by Topology: " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                } else {
                    weight = 0;
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Check disabled by user: " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                    }
                }
                if (input_weight != -3) {
                    AlertLevel AlertLevels = UserLevels.get(mtrscMetaLocal.getTags().get("UUID").getValue()); //new AlertLevel(true);
                    if (AlertLevels == null) {
                        AlertLevels = new AlertLevel(true);
                    }
                    Map<String, Object> metricsmap = new HashMap<>();
                    if (weight != 0) {
                        weight_per = weight_per / loop;
                        double predict_value = mtrscMetaLocal.getRegression().predict(CalendarObj.getTimeInMillis() / 1000);
                        double predict_value_per = 0;
                        if ((!Double.isNaN(predict_value)) && (predict_value != 0)) {
                            predict_value_per = (metric.getValue() - predict_value) / predict_value * 100;
                        }
                        metricsmap.put("level", AlertLevels.getErrorLevel(weight, weight_per, metric.getValue(), predict_value_per));
                        mtrscMetaLocal.getLevelList().add(AlertLevels.getErrorLevel(weight, weight_per, metric.getValue(), predict_value_per));
                        // TODO Karoxa hanel aradzin bolt
                        key = mtrscMetaLocal.getTags().get("UUID").getValueTSDBUID();
                        key = ArrayUtils.addAll(key, ByteBuffer.allocate(8).putLong((long) (CalendarObj.getTimeInMillis() / 1000)).array());
                        putvalue = new PutRequest(errortable, key, error_family, mtrscMetaLocal.getKey(), ByteBuffer.allocate(26).putShort((short) weight).putDouble(weight_per).putDouble(metric.getValue()).putDouble(predict_value_per).array());
                        globalFunctions.getSecindaryclient(clientconf).put(putvalue);
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Put Error" + weight + " " + CalendarObj.getTime() + "-" + mtrscMetaLocal.getName() + " " + mtrscMetaLocal.getTags());
                        }
                    } else {
                        metricsmap.put("lever", -1);
                        mtrscMetaLocal.getLevelList().add(-1);
                    }
                    if (mtrscMetaLocal.getLevelList().size() > 15) {
                        mtrscMetaLocal.getLevelList().remove(0);
                        mtrscMetaLocal.LevelValuesList().remove(0);
                    }

                    metricsmap.put("value", metric.getValue());
                    mtrscMetaLocal.LevelValuesList().add(metricsmap);

                    if (Collections.max(mtrscMetaLocal.getLevelList()) > -1) {
                        Map<Integer, Integer> Errormap = new TreeMap<>(Collections.reverseOrder());
                        mtrscMetaLocal.getLevelList().stream().forEachOrdered((e) -> {
                            for (int j = e; j >= 0; j--) {
                                Integer counter = Errormap.get(j);
                                if (counter != null) {
                                    counter++;
                                } else {
                                    counter = 1;
                                }

                                Errormap.put(j, counter);
                            }
                        });
                        if (!Errormap.isEmpty()) {
                            boolean setlevel = false;
                            boolean savelevel = true;
                            for (Map.Entry<Integer, Integer> item : Errormap.entrySet()) {
                                if (item.getValue() > AlertLevels.get(item.getKey()).get(AlertLevel.ALERT_PARAM_RECCOUNT)) {
                                    setlevel = true;
                                    if (mtrscMetaLocal.getErrorState().getLevel() < Iterables.getLast(mtrscMetaLocal.getLevelList())) {
                                        mtrscMetaLocal.getErrorState().setLevel(item.getKey(), metric.getTimestamp());
                                        savelevel = false;
                                        break;
                                    } else {
                                        if (Errormap.containsKey(mtrscMetaLocal.getErrorState().getLevel())) {
                                            try {
                                                //if (item.getValue() - Errormap.get(mtrscMetaLocal.getErrorState().getLevel()) > AlertLevels.get(item.getKey()).get(AlertLevel.ALERT_PARAM_RECCOUNT)) {
                                                if (item.getValue() > AlertLevels.get(item.getKey()).get(AlertLevel.ALERT_PARAM_RECCOUNT)) {
                                                    mtrscMetaLocal.getErrorState().setLevel(item.getKey(), metric.getTimestamp());
                                                    savelevel = false;
                                                    break;
                                                }
                                            } catch (Exception e) {
                                                LOGGER.error("Exception execute: " + globalFunctions.stackTrace(e));
                                            }
                                        }

                                    }

                                }

                            }
                            mtrscMetaLocal.getErrorState().setUpstate(weight > 0);
                            if (savelevel) {
                                mtrscMetaLocal.getErrorState().setLevel(mtrscMetaLocal.getErrorState().getLevel(), metric.getTimestamp());
                            }
                            if (!setlevel) {
                                mtrscMetaLocal.getErrorState().setLevel(AlertLevel.ALERT_END_ERROR, metric.getTimestamp());
                            }

                        }
                        collector.emit(new Values(mtrscMetaLocal.dublicate(), metric.dublicate()));
                    } else {
                        if (mtrscMetaLocal.getErrorState().getLevel() != -1) {
                            mtrscMetaLocal.getErrorState().setLevel(-1, metric.getTimestamp());
                            collector.emit(new Values(mtrscMetaLocal.dublicate(), metric.dublicate()));
                        }
                    }
                }
//                mtrscList.set(mtrscMetaLocal);
            }
        } catch (Exception ex) {
            LOGGER.error("Exception execute: " + globalFunctions.stackTrace(ex));
        }
    }
}
