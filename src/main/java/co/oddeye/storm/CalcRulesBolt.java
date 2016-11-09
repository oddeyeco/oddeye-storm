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
import com.stumbleupon.async.Deferred;
import java.io.IOException;
import java.util.ArrayList;
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
        try {
            OddeeyMetric metric = (OddeeyMetric) tuple.getValueByField("metric");
            collector.ack(tuple);
            OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
            if (mtrscList.containsKey(mtrsc.hashCode())) {
//                mtrscList.set(mtrsc);
//            } else {
                mtrsc = mtrscList.get(mtrsc.hashCode());
            }
            CalendarObjRules.setTimeInMillis(metric.getTimestamp());
            CalendarObjRules.add(Calendar.HOUR, 1);
            CalendarObjRules.add(Calendar.DATE, -1);

            Rules = mtrsc.getRules(CalendarObjRules, 7, metatable, globalFunctions.getClient(clientconf));
            needsave = false;
            final ArrayList<Deferred<DataPoints[]>> deferreds = new ArrayList<>();

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
                    //+ "-" + mtrsc.getName() + " " + mtrsc.getTags().get("host").getValue()
                }                
                try {

                    if (needsave) {

                        key = mtrsc.getKey();
                        byte[][] qualifiers;
                        byte[][] values;
                        ConcurrentMap<String, MetriccheckRule> rulesmap = mtrsc.getRulesMap();
                        qualifiers = new byte[rulesmap.size()][];
                        values = new byte[rulesmap.size()][];
                        int index = 0;

                        for (Map.Entry<String, MetriccheckRule> rule : rulesmap.entrySet()) {
                            qualifiers[index] = rule.getValue().getQualifier();
                            values[index] = rule.getValue().getValues();
                            index++;
                        }

                        if (qualifiers.length > 0) {
                            PutRequest putvalue = new PutRequest(metatable, key, family, qualifiers, values);                            
                            globalFunctions.getClient(clientconf).put(putvalue);
                            
                        } else {
                            PutRequest putvalue = new PutRequest(metatable, key, family, "n".getBytes(), key);
                            globalFunctions.getClient(clientconf).put(putvalue);
                        }
                    }
                    
                } catch (Exception e) {
                    LOGGER.error("catch In save " + globalFunctions.stackTrace(e));
                }

                mtrscList.set(mtrsc);
            }

        } catch (Exception ex) {
            LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
        }
    }

}