/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.AlertLevel;
import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.OddeeysSpecialMetric;
import co.oddeye.core.globalFunctions;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.opentsdb.utils.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CheckSpecialErrorBolt extends BaseRichBolt {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CheckSpecialErrorBolt.class);
    private OutputCollector collector;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private final Map conf;
    private byte[] metatable;
    private OddeeyMetricMetaList mtrscList;
    private final Map<Integer, OddeeysSpecialMetric> lastTimeSpecialMap = new HashMap<>();
    private final Map<Integer, OddeeysSpecialMetric> lastTimeSpecialLiveMap = new HashMap<>();
    private JsonParser parser = null;
    private JsonObject jsonResult = null;

    public CheckSpecialErrorBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mtrsc", "metric", "time"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
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
            globalFunctions.getTSDB(openTsdbConfig, clientconf);
//            CalendarObjRules = Calendar.getInstance();

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

            try {
                LOGGER.warn("Start read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable, true);
                int TaskId = context.getThisTaskId();
                int TaskIndex = context.getThisTaskIndex();
                List<Integer> tasks = context.getComponentTasks(context.getThisComponentId());
                for (Map.Entry<Integer, OddeeyMetricMeta> mtr : mtrscList.entrySet()) {
                    if ((mtr.getValue().isSpecial()) && (mtr.getValue().getLastreaction() > 0)) {
                        OddeeyMetricMeta mt = mtr.getValue();
                        if ((System.currentTimeMillis() - mt.getLasttime()) > Math.abs(60000 * mt.getLastreaction())) {
                            if (lastTimeSpecialLiveMap.get(mt.hashCode()) == null) {
                                final OddeeysSpecialMetric metric = new OddeeysSpecialMetric(mt);
                                if (tasks.get(Math.abs(metric.hashCode()) % tasks.size()) == TaskId) {
                                    LOGGER.warn("Task test: hash " + metric.hashCode() + " PPPP " + tasks.get(Math.abs(metric.hashCode()) % tasks.size()) + " TaskId:" + TaskId + " TaskIndex:" + TaskIndex);
                                    lastTimeSpecialLiveMap.put(mt.hashCode(), metric);
                                    mt.getErrorState().setLevel(AlertLevel.ALERT_LEVEL_SEVERE, System.currentTimeMillis());
                                }

                            }
                        }
                    }
                }
                LOGGER.warn("End read meta in hbase");
            } catch (Exception ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
                mtrscList = new OddeeyMetricMetaList();
            }

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");
        }
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals("SemaforProxyBolt")) {

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("message from SemaforProxyBolt" + input.getValueByField("action").toString());
            }

            jsonResult = this.parser.parse(input.getValueByField("action").toString()).getAsJsonObject();

            if (jsonResult.get("action").getAsString().equals("deletemetricbyhash")) {
                final int hash = jsonResult.get("hash").getAsInt();
                if (mtrscList.containsKey(hash)) {
                    mtrscList.remove(hash);
                    lastTimeSpecialLiveMap.remove(hash);
                    lastTimeSpecialMap.remove(hash);
                }
            }
        }

        if (input.getSourceComponent().equals("ParseSpecialMetricBolt")) {
            if (input.getValueByField("MetricField") instanceof Map) {
                Map<String, OddeeyMetric> MetricList = (Map<String, OddeeyMetric>) input.getValueByField("MetricField");
                for (Map.Entry<String, OddeeyMetric> metricEntry : MetricList.entrySet()) {
                    OddeeysSpecialMetric metric = (OddeeysSpecialMetric) metricEntry.getValue();
                    this.checkMetric(metric);
                }
            }

            if (input.getValueByField("MetricField") instanceof OddeeyMetric) {
                this.checkMetric((OddeeysSpecialMetric) input.getValueByField("MetricField"));
            }

        }

        if (input.getSourceComponent().equals("TimeSpout")) {
            for (Iterator<Map.Entry<Integer, OddeeysSpecialMetric>> it = lastTimeSpecialMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, OddeeysSpecialMetric> metricEntry = it.next();
                final OddeeysSpecialMetric metric = metricEntry.getValue();
                final Long lastTime = metric.getTimestamp();
                if ((System.currentTimeMillis() - lastTime) > Math.abs(60000 * metric.getReaction())) {
                    OddeeyMetricMeta mtrsc = mtrscList.get(metricEntry.getKey());
                    if (mtrsc == null) {
                        LOGGER.warn("for lastTimeSpecialMap Metric Meta not found " + metricEntry.getKey() + " metric name " + metric.getName() + " tags " + metric.getTags());
                        try {
                            mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
                        } catch (Exception ex) {
                            LOGGER.error(globalFunctions.stackTrace(ex));
                        }
                    }

                    if (mtrsc == null) {
                        LOGGER.warn("PIPEC for lastTimeSpecialMap Metric Meta not found " + metricEntry.getKey() + " metric name " + metric.getName() + " tags " + metric.getTags());
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags() + " count:" + lastTimeSpecialMap.size());
                        }
                        mtrsc.getErrorState().setLevel(AlertLevel.ALERT_END_ERROR, System.currentTimeMillis());
                        mtrscList.set(mtrsc);
                        collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));
                        it.remove();

                    }
                }
            }

            for (Iterator<Map.Entry<Integer, OddeeysSpecialMetric>> it = lastTimeSpecialLiveMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, OddeeysSpecialMetric> metricEntry = it.next();
                final OddeeysSpecialMetric metric = metricEntry.getValue();
                final Long lastTime = metric.getTimestamp();
                if ((System.currentTimeMillis() - lastTime) > Math.abs(60000 * metric.getReaction())) {
                    OddeeyMetricMeta mtrsc = mtrscList.get(metricEntry.getKey());
                    if (mtrsc == null) {
                        LOGGER.warn("for lastTimeSpecialLiveMap Metric Meta not found " + metricEntry.getKey() + " metric name " + metric.getName() + " tags " + metric.getTags());
                        try {
                            mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
                        } catch (Exception ex) {
                            LOGGER.error(globalFunctions.stackTrace(ex));
                        }
                    }
                    if (mtrsc == null) {
                        LOGGER.warn("PIPEC for lastTimeSpecialLiveMap Metric Meta not found " + metricEntry.getKey() + " metric name " + metric.getName() + " tags " + metric.getTags());
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags() + " count:" + lastTimeSpecialMap.size());
                        }
                        mtrsc.getErrorState().setLevel(AlertLevel.ALERT_LEVEL_SEVERE, System.currentTimeMillis());
                        mtrscList.set(mtrsc);
                        collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));
//                        it.remove();

                    }
                }
            }
        }
        this.collector.ack(input);
    }

    public void checkMetric(OddeeysSpecialMetric metric) {
        try {
//                OddeeysSpecialMetric metric = (OddeeysSpecialMetric) input.getValueByField("metric");
            OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));

            globalFunctions.saveMetric(mtrsc, metric, mtrscList, clientconf, metatable);

            mtrsc.setLasttime(metric.getTimestamp());
            mtrsc.getErrorState().setLevel(AlertLevel.getPyName(metric.getStatus()), metric.getTimestamp());
            if (metric.getReaction() == 0) {
                lastTimeSpecialLiveMap.remove(mtrsc.hashCode());
                lastTimeSpecialMap.remove(mtrsc.hashCode());

            }
            if (metric.getReaction() > 0) {
                if (lastTimeSpecialLiveMap.get(mtrsc.hashCode()) == null) {
                    lastTimeSpecialLiveMap.put(mtrsc.hashCode(), metric);
                } else if (lastTimeSpecialLiveMap.get(mtrsc.hashCode()).getTimestamp() < metric.getTimestamp()) {
                    lastTimeSpecialLiveMap.put(mtrsc.hashCode(), metric);
                }
                lastTimeSpecialMap.remove(mtrsc.hashCode());
            } else if (metric.getReaction() < 0) {
                lastTimeSpecialMap.put(mtrsc.hashCode(), metric);
                lastTimeSpecialLiveMap.remove(mtrsc.hashCode());
            }
//                }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(" Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " Oldlevel:" + mtrsc.getErrorState().getLevel() + " Newlevel:" + AlertLevel.getPyName(metric.getStatus()) + "Tags:" + mtrsc.getTags());
            }
//                mtrsc.getErrorState().setLevel(AlertLevel.getPyName(metric.getStatus()), metric.getTimestamp());

            collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));

            mtrscList.set(mtrsc);
        } catch (Exception ex) {
            LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
        }
    }
}
