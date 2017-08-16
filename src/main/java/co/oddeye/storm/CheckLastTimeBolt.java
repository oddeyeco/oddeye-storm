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
import co.oddeye.core.globalFunctions;
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
public class CheckLastTimeBolt extends BaseRichBolt {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CheckLastTimeBolt.class);
    private OutputCollector collector;
    private final Map<Integer, Long> lastTimeLiveMap = new HashMap<>();
    private final Map<Integer, Long> lastTimeSpecialMap = new HashMap<>();
    private final Map conf;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] metatable;
    private OddeeyMetricMetaList mtrscList;
//    private OddeeyMetricMeta mtrsc;

    public CheckLastTimeBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mtrsc", "metric", "time"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
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
            clientconf.overrideConfig("hbase.rpcs.batch.size", String.valueOf(conf.get("hbase.rpcs.batch.size")));
            globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf);
//            CalendarObjRules = Calendar.getInstance();

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

            try {

                LOGGER.warn("Start read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf), this.metatable, "host_absent");
//                List<Integer> tasks = context.getComponentTasks(context.getComponentId(context.getThisTaskId()));
//                LOGGER.warn("End read meta in hbase for CheckLastTimeBolt" + mtrscList.size());

//                LOGGER.warn("getThisTaskIndex " + context.getThisTaskId());                
//                LOGGER.warn("getThisTaskIndex 2 " + tasks);
//                final Iterator<Map.Entry<Integer, OddeeyMetricMeta>> it;
//                for (it = mtrscList.entrySet().iterator(); it.hasNext();) {
//                    final Map.Entry<Integer, OddeeyMetricMeta> metricEntry = it.next();
////                    LOGGER.warn("getTags().hashCode" + metricEntry.getValue().getTags().hashCode() + " - " + tasks.get(Math.abs(metricEntry.getValue().getTags().hashCode()) % tasks.size()));
//                    if ((tasks.get(Math.abs(metricEntry.getValue().getTags().hashCode()) % tasks.size())) == context.getThisTaskId()) {
//                        lastTimeLiveMap.put(metricEntry.getValue().hashCode(), metricEntry.getValue().getLasttime());
//                    }
//                }
            } catch (Exception ex) {
                mtrscList = new OddeeyMetricMetaList();
                LOGGER.error("Read meta Exception: " + globalFunctions.stackTrace(ex));
            }

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB execption : " + globalFunctions.stackTrace(ex));

        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");
    }

    @Override
    public void execute(Tuple input) {

        if (input.getSourceComponent().equals("FilterForLastTimeBolt")) {
            OddeeyMetric metric = (OddeeyMetric) input.getValueByField("metric");
            try {
                final OddeeyMetricMeta mtrsctmp = new OddeeyMetricMeta(metric, globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf));
                if (!mtrscList.containsKey(mtrsctmp.hashCode())) {
                    mtrscList.set(mtrsctmp);
//                    LOGGER.warn("Add metric to list" + System.currentTimeMillis() + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue() + " State:" + mtrsc.getErrorState().getLevel());
                }
                final OddeeyMetricMeta mtrsc = mtrscList.get(mtrsctmp.hashCode());
//                if (metric instanceof OddeeysSpecialMetric) {
//                    if (LOGGER.isDebugEnabled()) {
//                        LOGGER.debug("OddeeysSpecialMetric: Name:" + metric.getName() + " tags:" + metric.getTags());
//                    }
//                    lastTimeSpecialMap.put(mtrsc.hashCode(), metric.getTimestamp());
//                    mtrsc.getErrorState().setLevel(AlertLevel.ALERT_LEVEL_SEVERE, System.currentTimeMillis());
//                } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Add to lastTimeLiveMap Name:" + metric.getName() + " tags:" + metric.getTags());
                }
                if (lastTimeLiveMap.containsKey(mtrsc.hashCode())) {
                    if (lastTimeLiveMap.get(mtrsc.hashCode()) < metric.getTimestamp()) {
                        lastTimeLiveMap.put(mtrsc.hashCode(), metric.getTimestamp());
                    }
                } else {
                    lastTimeLiveMap.put(mtrsc.hashCode(), metric.getTimestamp());
                }
                if (mtrsc.getErrorState().getLevel() != -1) {
                    mtrsc.getErrorState().setLevel(-1, System.currentTimeMillis());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("end Live error" + System.currentTimeMillis() + " hashCode:" + mtrsc.getTags().hashCode() + " Host:" + mtrsc.getTags().get("host").getValue() + " State:" + mtrsc.getErrorState().getState());
                    }
//                    LOGGER.warn("end Live error" + System.currentTimeMillis() + " hashCode:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue() + " State:" + mtrsc.getErrorState().getState() + " Time:" + mtrsc.getErrorState().getTime());
                    if (mtrsc.getErrorState().getState() != 1) {
                        collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));
                    }
                }
//                }
            } catch (Exception ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            }

            // Todo Fix last time
        } else if (input.getSourceComponent().equals("TimerSpout")) {
            LOGGER.info("Start sheduler");

//            for (Iterator<Map.Entry<Integer, Long>> it = lastTimeSpecialMap.entrySet().iterator(); it.hasNext();) {
//                Map.Entry<Integer, Long> lastTime = it.next();
//                if (System.currentTimeMillis() - lastTime.getValue() > 60000 * 2) {
//                    mtrsc = mtrscList.get(lastTime.getKey());
//                    if (mtrsc == null) {
//                        LOGGER.warn("Metric not found " + lastTime.getKey());
//                    } else {
//                        if (LOGGER.isInfoEnabled()) {
//                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime.getValue() + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue());
//                        }
//                        mtrsc.getErrorState().setLevel(-1, System.currentTimeMillis());
//                        it.remove();
//                        collector.emit(new Values(mtrsc, null, System.currentTimeMillis()));
//
//                    }
//                }
//            }
            for (Iterator<Map.Entry<Integer, Long>> it = lastTimeLiveMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, Long> lastTime = it.next();
                final OddeeyMetricMeta mtrsc = mtrscList.get(lastTime.getKey());
                if (mtrsc == null) {
                    LOGGER.warn("Metric not found " + lastTime.getKey());
                } else {
                    if (System.currentTimeMillis() - lastTime.getValue() > 60000 * 3) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("start Live error" + System.currentTimeMillis() + " " + lastTime.getValue() + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue());
                        }
                        if (mtrsc.getErrorState().getLevel() != AlertLevel.ALERT_LEVEL_SEVERE) {
                            mtrsc.getErrorState().setLevel(AlertLevel.ALERT_LEVEL_SEVERE, lastTime.getValue());
                            it.remove();
                            collector.emit(new Values(mtrsc, null, System.currentTimeMillis()));
                        }
                    }
                }
                if (mtrsc != null) {
                    mtrscList.set(mtrsc);
                }
            }

        }

        //ToDo Check last time
        collector.ack(input);
    }

}
