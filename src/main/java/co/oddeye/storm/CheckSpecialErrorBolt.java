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
import java.io.IOException;
import java.nio.ByteBuffer;
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

    public CheckSpecialErrorBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mtrsc", "metric"));
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
    public void execute(Tuple input) {
        try {
            OddeeysSpecialMetric metric = (OddeeysSpecialMetric) input.getValueByField("metric");
            OddeeyMetricMeta mtrsc = new OddeeyMetricMeta(metric, globalFunctions.getTSDB(openTsdbConfig, clientconf));
            PutRequest putvalue;
            key = mtrsc.getKey();
            if (!mtrscList.containsKey(mtrsc.hashCode())) {
                mtrsc.getRegression().addData(metric.getTimestamp(), metric.getValue());
                qualifiers = new byte[3][];
                values = new byte[3][];
                qualifiers[0] = "n".getBytes();
                qualifiers[1] = "timestamp".getBytes();
                qualifiers[2] = "Special".getBytes();
                values[0] = key;
                values[1] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                values[2] = new byte[]{1};
                putvalue = new PutRequest(metatable, key, meta_family, qualifiers, values);
            } else {
                mtrsc = mtrscList.get(mtrsc.hashCode());
                qualifiers = new byte[1][];
                values = new byte[1][];
                qualifiers[0] = "timestamp".getBytes();
                values[0] = ByteBuffer.allocate(8).putLong(metric.getTimestamp()).array();
                putvalue = new PutRequest(metatable, mtrsc.getKey(), meta_family, qualifiers, values);
                LOGGER.info("Update timastamp:" + mtrsc.getName() + " tags " + mtrsc.getTags() + " Stamp " + metric.getTimestamp());
            }
            globalFunctions.getSecindaryclient(clientconf).put(putvalue);

//            mtrsc.getErrorState().setLevel(-1, metric.getTimestamp());

            mtrsc.getErrorState().setLevel(AlertLevel.getPyName(metric.getType()), metric.getTimestamp());

            collector.emit(new Values(mtrsc, metric));
            
            mtrscList.set(mtrsc);
        } catch (Exception ex) {
            LOGGER.error("in bolt: " + globalFunctions.stackTrace(ex));
        }
        collector.ack(input);
    }

}
