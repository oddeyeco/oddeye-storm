/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.MetriccheckRule;
import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.globalFunctions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import net.opentsdb.utils.Config;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
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
    private byte[] metatable;
    private byte[] key;
    private Calendar CalendarObjRules;
    private Calendar CalendarObj;
    private Map<String, MetriccheckRule> Rules;
    private int weight;
    private int curent_DW;
    private MetriccheckRule Rule;
    private int local_DW;
    private int weight_KF;
//    private int weight_D_KF;
    private int devkef = 1;

    private byte[] errortable;
    private final byte[] error_family = "d".getBytes();
    private OddeeyMetricMeta oldmtrc;

    /**
     *
     * @param config
     */
    public CompareBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("rule"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        LOGGER.warn("DoPrepare WriteToTSDBseries");
        collector = oc;

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
            globalFunctions.getTSDB(openTsdbConfig, clientconf);

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

//            CalendarObjRules = Calendar.getInstance();
            CalendarObj = Calendar.getInstance();
            try {
                LOGGER.warn("Start read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable);
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
        try {
            OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
            collector.ack(tuple);
            OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
            PutRequest putvalue;
            key = mtrsc.getKey();
            if (!mtrscList.containsKey(mtrsc.hashCode())) {                
                byte[][] qualifiers = new byte[2][];
                byte[][] values = new byte[2][];                
                qualifiers[0] = "n".getBytes();
                qualifiers[1] = "timestamp".getBytes();
                values[0]=key;
                values[1]=ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                putvalue = new PutRequest(metatable, key, meta_family, qualifiers, values);                
                LOGGER.info("Add metric Meta to hbase:" + mtrsc.getName() + " tags " + mtrsc.getTags());
            } else {
                oldmtrc = mtrsc;
                mtrsc = mtrscList.get(mtrsc.hashCode());
                if (!Arrays.equals(mtrsc.getKey(), key)) {
                    LOGGER.warn("More key for single hash:" + mtrsc.getName() + " tags " + mtrsc.getTags() + "More key for single hash:" + oldmtrc.getName() + " tags " + oldmtrc.getTags() + " mtrsc.getKey() = " + Hex.encodeHexString(mtrsc.getKey()) + " Key= " + Hex.encodeHexString(key));
                }                                      
                putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, "timestamp".getBytes(), ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array() );
                LOGGER.info("Update timastamp:" + mtrsc.getName() + " tags " + mtrsc.getTags() + " Stamp "+ metric.getTimestamp());
            }
            globalFunctions.getClient(clientconf).put(putvalue);
            
            CalendarObj.setTimeInMillis(metric.getTimestamp());
            Rules = mtrsc.getRules(CalendarObj, 7, metatable, globalFunctions.getClient(clientconf));
            String alert_level = metric.getTags().get("alert_level");
            short p_weight = 0;
            if (null != alert_level) {
                p_weight = (short) Double.parseDouble(alert_level);
            }

            if ((alert_level == null) || ((p_weight < 1) && (p_weight > -3))) {
//            if (false) {    
                weight = 0;
                curent_DW = CalendarObj.get(Calendar.DAY_OF_WEEK);
                LOGGER.info(CalendarObj.getTime() + "-" + metric.getName() + " " + metric.getTags().get("host"));
                for (Map.Entry<String, MetriccheckRule> RuleEntry : Rules.entrySet()) {
                    Rule = RuleEntry.getValue();
                    if (Rule == null) {
                        LOGGER.warn("Rule is NUll: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                        continue;
                    }
                    CalendarObjRules = MetriccheckRule.QualifierToCalendar(Rule.getQualifier());
//                    if ((!Rule.isIsValidRule()) && (!Rule.isHasNotData())) {
//                        collector.emit(new Values(Rule));
//                    }

                    if (!Rule.isIsValidRule()) {
                        LOGGER.info("No rule for check in cache: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                        continue;
                    }
                    if (Rule.isHasNotData()) {
                        LOGGER.info("rule Has no data for check in cache: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                        continue;
                    }
                    
//                                        LOGGER.warn("Rule: " + Rule);
                    local_DW = CalendarObjRules.get(Calendar.DAY_OF_WEEK);
                    if (curent_DW == local_DW) {
                        weight_KF = 2;
//                        weight_D_KF = -1;
                    } else {
                        weight_KF = 1;
//                        weight_D_KF = 0;
                    }
//                                        LOGGER.warn("Rule: " +Rule.toString());
                    if (p_weight != -1) {
                        if (Rule.getAvg() != null && Rule.getDev() != null) {
                            if (metric.getValue() > Rule.getAvg() + devkef * Rule.getDev()) {
                                weight = (short) (weight + weight_KF);
                            }
                        }
                        if (Rule.getMax() != null) {
                            if (metric.getValue() > Rule.getMax()) {
                                weight = (short) (weight + weight_KF);
                            }
                        }
                    } else {
                        LOGGER.info("Check Up Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                    }

                    if (p_weight != -2) {
                        if (Rule.getMin() != null) {
                            if (metric.getValue() < Rule.getMin()) {
                                weight = (short) (weight - weight_KF);
                            }
                        }
                        if (Rule.getAvg() != null && Rule.getDev() != null) {
                            if (metric.getValue() < Rule.getAvg() - devkef * Rule.getDev()) {
                                weight = (short) (weight - weight_KF);
                            }
                        }
                    } else {
                        LOGGER.info("Check Down Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
                    }

                }

                p_weight = (short) weight;
            } else if (p_weight > 0) {
                if (metric.getValue() > p_weight) {
                    p_weight = 16;
                } else {
                    p_weight = 0;
                }
            } else if (p_weight == -4) {
                LOGGER.info("Check disabled by so old messge: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
            } else if (p_weight == -5) {
                LOGGER.warn("Check disabled by Topology: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
            } else {
                LOGGER.info("Check disabled by user: " + CalendarObj.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
            }
            mtrscList.set(mtrsc);
            if (p_weight != 0) {
                // TODO Karoxa hanel radzin bolt
                key = mtrsc.getTags().get("UUID").getValueTSDBUID();
                key = ArrayUtils.addAll(key, ByteBuffer.allocate(8).putLong((long) (CalendarObj.getTimeInMillis() / 1000)).array());

                putvalue = new PutRequest(errortable, key, error_family, mtrsc.getKey(), ByteBuffer.allocate(2).putShort(p_weight).array());
                globalFunctions.getClient(clientconf).put(putvalue);
            }
        } catch (Exception ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }
}
