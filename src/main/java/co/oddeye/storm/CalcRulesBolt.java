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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.stumbleupon.async.Deferred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import net.opentsdb.core.DataPoints;
import net.opentsdb.utils.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.PutRequest;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CalcRulesBolt extends BaseRichBolt {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CalcRulesBolt.class);
    private final Map conf;
    private OutputCollector collector;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] metatable;
    private OddeeyMetricMetaList mtrscList;
    private Calendar CalendarObjRules;
    private Map<String, MetriccheckRule> Rules;
    private boolean needsave;
    private long starttime;
    private long endtime;
    private byte[] key;
    private final byte[] family = "d".getBytes();

    private JsonParser parser = null;
    private JsonObject jsonResult = null;

    /**
     *
     * @param config
     */
    public CalcRulesBolt(java.util.Map config) {
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
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");
            globalFunctions.getTSDB(openTsdbConfig, clientconf);
            CalendarObjRules = Calendar.getInstance();

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

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
        this.collector.ack(tuple);
        if (tuple.getSourceComponent().equals("SemaforProxyBolt")) {            
            
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("message from SemaforProxyBolt" + tuple.getValueByField("action").toString());
            }                           
            
            jsonResult = this.parser.parse(tuple.getValueByField("action").toString()).getAsJsonObject();

            if (jsonResult.get("action").getAsString().equals("deletemetricbyhash")) {
                final int hash = jsonResult.get("hash").getAsInt();
                if (mtrscList.containsKey(hash)) {
                    mtrscList.remove(hash);
                }
            }
        }

        if (tuple.getSourceComponent().equals("ParseMetricBolt")) {
            try {
                OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
//                this.collector.ack(tuple);
                if (metric != null) {
                    OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
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
                        code = mtrsc.hashCode();
                    } catch (Exception ex) {
                        LOGGER.error("In hashCode: " + metric.getName() + " " + globalFunctions.stackTrace(ex));
                    }

                    if (code != 0) {
                        if (mtrscList.containsKey(mtrsc.hashCode())) {
                            mtrsc = mtrscList.get(mtrsc.hashCode());
                        }
                        try {
                            calcRules(mtrsc, metric, code);
                            mtrscList.set(mtrsc);
                        } catch (Exception ex) {
                            LOGGER.error("in metric: " + globalFunctions.stackTrace(ex));
                        }
                    } else {
                        LOGGER.error("code is 0: ");
                    }
                } else {
                    LOGGER.error("metric is null: ");
                    LOGGER.error(tuple.getFields().size() + "");
                    LOGGER.error(tuple.getValueByField("metric").toString());
                }
//            }
            } catch (Exception ex) {
                LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
            }
        }

    }

    private void calcRules(OddeeyMetricMeta mtrsc, OddeeyMetric metric, Integer code) throws Exception {

        if (metric == null) {
            LOGGER.warn("Metric Null Hash:" + code);
            return;
        }

        if (metric.getTimestamp() == null) {
            LOGGER.warn("Metric getTimestamp Null Hash:" + code);
        }
        CalendarObjRules.setTimeInMillis(metric.getTimestamp());
        CalendarObjRules.add(Calendar.HOUR, 1);
        CalendarObjRules.add(Calendar.DATE, -1);

        Rules = mtrsc.getRules(CalendarObjRules, 7, metatable, globalFunctions.getClient(clientconf));
        needsave = false;
        final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>();
        mtrsc.clearCalcedRulesMap();
        for (Map.Entry<String, MetriccheckRule> RuleEntry : Rules.entrySet()) {
            final MetriccheckRule l_Rule = RuleEntry.getValue();
            Calendar CalObjRules = MetriccheckRule.QualifierToCalendar(l_Rule.getQualifier());
            Calendar CalObjRulesEnd = (Calendar) CalObjRules.clone();
            CalObjRulesEnd.add(Calendar.HOUR, 1);
            CalObjRulesEnd.add(Calendar.MILLISECOND, -1);
            if ((!l_Rule.isIsValidRule()) && (!l_Rule.isHasNotData())) {

                ArrayList<Deferred<DataPoints[]>> rule_deferreds = mtrsc.CalculateRulesApachMath(CalObjRules.getTimeInMillis(), CalObjRulesEnd.getTimeInMillis(), globalFunctions.getTSDB(openTsdbConfig, clientconf));

                deferreds.addAll(rule_deferreds);
            }
            if (deferreds.size() > 0) {
                needsave = true;
                starttime = System.currentTimeMillis();
                Deferred.groupInOrder(deferreds).joinUninterruptibly();
                endtime = System.currentTimeMillis() - starttime;
                LOGGER.info("Rule joinUninterruptibly " + CalendarObjRules.getTime() + " to 1 houre time: " + endtime + " Name:" + mtrsc.getName() + " host" + mtrsc.getTags().get("host").getValue());
            } else {
                LOGGER.info("All Rule is Exist: " + CalendarObjRules.getTime() + "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue());
            }
            try {

                if (needsave) {
                    key = mtrsc.getKey();
                    byte[][] qualifiers;
                    byte[][] values;
                    ConcurrentMap<String, MetriccheckRule> rulesmap = mtrsc.getCalcedRulesMap();
                    qualifiers = new byte[rulesmap.size()][];
                    values = new byte[rulesmap.size()][];
                    int index = 0;

                    for (Map.Entry<String, MetriccheckRule> rule : rulesmap.entrySet()) {
                        if (rule.getValue().getQualifier() == null) {
                            qualifiers[index] = "null".getBytes();
                            LOGGER.error("qualifiers is null " + " Hash: " + mtrsc.hashCode() + " index:" + index);
                        } else {
                            qualifiers[index] = rule.getValue().getQualifier();
                        }
                        if (rule.getValue().getValues() == null) {
                            values[index] = "null".getBytes();
                            LOGGER.error("values is null " + " Hash: " + mtrsc.hashCode() + " index:" + index);
                        } else {
                            values[index] = rule.getValue().getValues();
                        }

                        index++;
                    }

                    if (qualifiers.length > 0) {
                        try {
                            PutRequest putvalue = new PutRequest(metatable, key, family, qualifiers, values);
                            globalFunctions.getClient(clientconf).put(putvalue);
                        } catch (Exception e) {
                            LOGGER.warn("catch In Multi qualifiers index: " + index + "rulesmap.size" + rulesmap.size() + " qualifiers.length " + qualifiers.length);
                            LOGGER.warn("catch In Multi qualifiers metatable: " + Arrays.toString(metatable) + " key " + Arrays.toString(key) + "family" + family);
                            LOGGER.warn("catch In Multi qualifiers Hash: " + mtrsc.hashCode() + " qualifiers " + Arrays.deepToString(qualifiers) + "values" + Arrays.deepToString(values));
                            LOGGER.error("catch In Multi qualifiers stackTrace: " + globalFunctions.stackTrace(e));

                        }
                    } else {
                        try {
                            PutRequest putvalue = new PutRequest(metatable, key, family, "n".getBytes(), key);
                            globalFunctions.getClient(clientconf).put(putvalue);
                        } catch (Exception e) {
                            LOGGER.error("catch In Single qualifiers " + globalFunctions.stackTrace(e) + " qualifiers " + qualifiers + "values" + values);
                        }

                    }

                }

            } catch (Exception e) {
                LOGGER.error("catch In save " + globalFunctions.stackTrace(e));
            }
        }

    }

}
