/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.AlertLevel;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import co.oddeye.core.OddeeysSpecialMetric;
import co.oddeye.core.globalFunctions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.opentsdb.utils.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.hbase.async.PutRequest;
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
    private byte[] key;
    private byte[][] qualifiers;
    private byte[][] values;
    private final byte[] meta_family = "d".getBytes();
    private final Map<Integer, OddeeysSpecialMetric> lastTimeSpecialMap = new HashMap<>();
    private final Map<Integer, OddeeysSpecialMetric> lastTimeSpecialLiveMap = new HashMap<>();

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
//            CalendarObjRules = Calendar.getInstance();

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

            try {
                LOGGER.warn("Start read meta in hbase");
//                final OddeeyMetricMetaList mtrscListtmp = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable,true);
                LOGGER.warn("End read meta in hbase");
                mtrscList = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable, true);
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
        if (input.getSourceComponent().equals("ParseSpecialMetricBolt")) {
            try {
                OddeeysSpecialMetric metric = (OddeeysSpecialMetric) input.getValueByField("metric");
                OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
                PutRequest putvalue;
                key = mtrsc.getKey();
                if (!mtrscList.containsKey(mtrsc.hashCode())) {
                    qualifiers = new byte[3][];
                    values = new byte[3][];
                    qualifiers[0] = "n".getBytes();
                    qualifiers[1] = "timestamp".getBytes();
                    qualifiers[2] = "type".getBytes();
                    values[0] = key;
                    values[1] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                    values[2] = ByteBuffer.allocate(2).putShort(metric.getType()).array();
                    putvalue = new PutRequest(metatable, key, meta_family, qualifiers, values);
                } else {
                    mtrsc = mtrscList.get(mtrsc.hashCode());
                    qualifiers = new byte[1][];
                    values = new byte[1][];
                    if (metric.getType() != mtrsc.getType()) {
                        qualifiers = new byte[2][];
                        values = new byte[2][];
                        qualifiers[1] = "type".getBytes();
                        values[1] = ByteBuffer.allocate(2).putShort(metric.getType()).array();
                        mtrsc.setType(metric.getType());
                    }
                    qualifiers[0] = "timestamp".getBytes();
                    values[0] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                    putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifiers, values);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Update timastamp:" + mtrsc.getName() + " tags " + mtrsc.getTags() + " Stamp " + metric.getTimestamp());
                    }
                }
                globalFunctions.getSecindaryclient(clientconf).put(putvalue);

                mtrsc.getErrorState().setLevel(AlertLevel.getPyName(metric.getStatus()), metric.getTimestamp());

                if (metric.getReaction() > 0) {                    
                    lastTimeSpecialLiveMap.put(mtrsc.hashCode(), metric);                    
                } else if (metric.getReaction() < 0) {                                        
//                    LOGGER.warn("metric.getReaction() < 0) Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " Oldlevel:" + mtrsc.getErrorState().getLevel() + " Newlevel:" + AlertLevel.getPyName(metric.getStatus()) + "Tags:" + mtrsc.getTags());
                    lastTimeSpecialMap.put(mtrsc.hashCode(), metric);
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

        if (input.getSourceComponent().equals("TimerSpout")) {
            for (Iterator<Map.Entry<Integer, OddeeysSpecialMetric>> it = lastTimeSpecialMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, OddeeysSpecialMetric> metricEntry = it.next();
                final OddeeysSpecialMetric metric = metricEntry.getValue();
                final Long lastTime = metric.getTimestamp();
                if ((System.currentTimeMillis() - lastTime) > Math.abs(60000 * metric.getReaction())) {
                    final OddeeyMetricMeta mtrsc = mtrscList.get(metricEntry.getKey());
                    if (mtrsc == null) {
                        LOGGER.warn("Metric Meta not found " + metricEntry.getKey());
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue() + " count:" + lastTimeSpecialMap.size());
                        }
                        mtrsc.getErrorState().setLevel(AlertLevel.ALERT_END_ERROR, System.currentTimeMillis());
                        mtrscList.set(mtrsc);
//                        LOGGER.warn("metric.getReaction() < 0) Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " Oldlevel:" + mtrsc.getErrorState().getLevel() + " Newlevel:" + AlertLevel.getPyName(metric.getStatus()) + "Tags:" + mtrsc.getTags());
                        it.remove();
                        collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));
                    }
                }
            }

            for (Iterator<Map.Entry<Integer, OddeeysSpecialMetric>> it = lastTimeSpecialLiveMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, OddeeysSpecialMetric> metricEntry = it.next();
                final OddeeysSpecialMetric metric = metricEntry.getValue();
                final Long lastTime = metric.getTimestamp();
                if ((System.currentTimeMillis() - lastTime) > Math.abs(60000 * metric.getReaction())) {
                    final OddeeyMetricMeta mtrsc = mtrscList.get(metricEntry.getKey());
                    if (mtrsc == null) {
                        LOGGER.warn("Metric Meta not found " + metricEntry.getKey());
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue() + " count:" + lastTimeSpecialMap.size());
                        }
                        mtrsc.getErrorState().setLevel(AlertLevel.ALERT_LEVEL_SEVERE, System.currentTimeMillis());
                        mtrscList.set(mtrsc);
                        it.remove();
                        collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));
                    }
                }
            }
        }
        collector.ack(input);
    }

}
