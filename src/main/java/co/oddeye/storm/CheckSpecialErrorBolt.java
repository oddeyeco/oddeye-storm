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
    private final Map<Integer, Long> lastTimeSpecialMap = new HashMap<>();

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
                final OddeeyMetricMetaList mtrscListtmp = new OddeeyMetricMetaList(globalFunctions.getTSDB(openTsdbConfig, clientconf), this.metatable);
                LOGGER.warn("End read meta in hbase");
                mtrscList = new OddeeyMetricMetaList();
                mtrscListtmp.entrySet().stream().filter((mtrsc) -> (mtrsc.getValue().isSpecial())).forEachOrdered((mtrsc) -> {
//                    if (!mtrsc.getValue().getName().equals("host_absent")) {
                        lastTimeSpecialMap.put(mtrsc.getValue().hashCode(), mtrsc.getValue().getLasttime());
                        mtrscList.set(mtrsc.getValue());
//                    }
                });
            } catch (Exception ex) {
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
                    qualifiers[0] = "timestamp".getBytes();
                    values[0] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                    putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifiers, values);
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Update timastamp:" + mtrsc.getName() + " tags " + mtrsc.getTags() + " Stamp " + metric.getTimestamp());
                    }
                }
                globalFunctions.getSecindaryclient(clientconf).put(putvalue);

                if (AlertLevel.getPyName(metric.getStatus()) != -1) {
                    lastTimeSpecialMap.put(mtrsc.hashCode(), metric.getTimestamp());
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(" Name:" + mtrsc.getName() + " State:" + mtrsc.getErrorState().getState() + " Oldlevel:" + mtrsc.getErrorState().getLevel() + " Newlevel:" + AlertLevel.getPyName(metric.getStatus()) + "Tags:" + mtrsc.getTags());
                }
                mtrsc.getErrorState().setLevel(AlertLevel.getPyName(metric.getStatus()), metric.getTimestamp());

                collector.emit(new Values(mtrsc, metric, System.currentTimeMillis()));

                mtrscList.set(mtrsc);
            } catch (Exception ex) {
                LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
            }
        }
        if (input.getSourceComponent().equals("TimerSpout")) {
            for (Iterator<Map.Entry<Integer, Long>> it = lastTimeSpecialMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<Integer, Long> lastTime = it.next();
                if (System.currentTimeMillis() - lastTime.getValue() > 60000 * 2) {
                    final OddeeyMetricMeta mtrsc = mtrscList.get(lastTime.getKey());
                    if (mtrsc == null) {
                        LOGGER.warn("Metric not found " + lastTime.getKey());
                    } else {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("end error" + System.currentTimeMillis() + " " + lastTime.getValue() + " Name:" + mtrsc.getName() + " Host:" + mtrsc.getTags().get("host").getValue() + " count:" + lastTimeSpecialMap.size());
                        }
                        mtrsc.getErrorState().setLevel(-1, System.currentTimeMillis());
                        it.remove();
                        collector.emit(new Values(mtrsc, null, System.currentTimeMillis()));
                    }
                }
            }
        }
        collector.ack(input);
    }

}
