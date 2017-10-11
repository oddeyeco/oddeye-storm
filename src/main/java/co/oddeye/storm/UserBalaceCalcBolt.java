/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.globalFunctions;
import co.oddeye.storm.core.StormUser;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.Timer;
import java.util.TreeMap;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.commons.lang.ArrayUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class UserBalaceCalcBolt extends BaseRichBolt {

    protected OutputCollector collector;

    public static final Logger LOGGER = LoggerFactory.getLogger(UserBalaceCalcBolt.class);
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] usertable;
    private byte[] consumptiontable;
    private final byte[] consumptionfamily = "c".getBytes();
    private JsonParser parser;
    private HashMap<String, StormUser> UserList;
    double messageprice;

    private final java.util.Map<String, Object> conf;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    class SaveTask extends TimerTask {

        @Override
        public void run() {
            LOGGER.warn("Write 10 minutes consumption to" + (new String(consumptiontable)));
            try {
                for (Map.Entry<String, StormUser> userEntry : UserList.entrySet()) {
                    if (userEntry.getValue().getTmpconsumption().getAmount() > 0) {
                        LOGGER.warn(userEntry.getValue().getEmail() + " " + userEntry.getValue().getTmpconsumption().getAmount() + " " + userEntry.getValue().getTmpconsumption().getCount());
                        Calendar cal = Calendar.getInstance();
                        cal.set(Calendar.MILLISECOND, 0);
                        cal.set(Calendar.SECOND, 0);
                        byte[] month_key = ByteBuffer.allocate(4).putInt(cal.get(Calendar.MONTH)).array();
                        byte[] day_key = ByteBuffer.allocate(4).putInt(cal.get(Calendar.DATE)).array();
                        byte[] houre_key = ByteBuffer.allocate(4).putInt(cal.get(Calendar.HOUR)).array();
                        byte[] minute_key = ByteBuffer.allocate(4).putInt(cal.get(Calendar.MINUTE)).array();
                        byte[] key = ArrayUtils.addAll(userEntry.getValue().getId().toString().getBytes(), month_key);
                        byte[] qualifiers = ArrayUtils.addAll(ArrayUtils.addAll(day_key, houre_key), minute_key);
                        byte[] values = ByteBuffer.allocate(12).putDouble(userEntry.getValue().getTmpconsumption().getAmount()).putInt(userEntry.getValue().getTmpconsumption().getCount()).array();
                        userEntry.getValue().getTmpconsumption().clear();
                        PutRequest putvalue = new PutRequest(consumptiontable, key, consumptionfamily, qualifiers, values);
                        globalFunctions.getClient(clientconf).put(putvalue);

                    } else {
                        LOGGER.warn(userEntry.getValue().getEmail() + " EMPTY");
                    }

                }
            } finally {
                LOGGER.warn("Write end 10 minutes consumption");
            }

        }
    }

    public UserBalaceCalcBolt(java.util.Map config) {
        this.usertable = "oddeyeusers".getBytes();
        this.consumptiontable = "oddeye-consumption".getBytes();

        this.conf = config;

        this.usertable = String.valueOf(this.conf.get("usertable")).getBytes();
        this.consumptiontable = String.valueOf(conf.get("consumptionusertable")).getBytes();
        this.messageprice = Double.parseDouble(String.valueOf(conf.get("messageprice")));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try {
            collector = oc;
            parser = new JsonParser();
            UserList = new HashMap<>();
            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", String.valueOf(conf.get("hbase.rpcs.batch.size")));

            TSDB tsdb = globalFunctions.getSecindarytsdb(openTsdbConfig, clientconf);
            if (tsdb == null) {
                LOGGER.error("tsdb: " + tsdb);
            }

            final Scanner user_scanner = globalFunctions.getSecindaryclient(clientconf).newScanner(usertable);
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = user_scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    final StormUser User = new StormUser(row, parser);
                    UserList.put(User.getId().toString(), User);
                }
            }
            LOGGER.warn("UserList.size " + UserList.size());

        } catch (IOException ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        } catch (Exception ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceComponent().equals("TimerSpout10x")) {
            (new SaveTask()).run();
        }
        if ((tuple.getSourceComponent().equals("ParseMetricBolt")) || (tuple.getSourceComponent().equals("ParseSpecialMetricBolt"))) {
            StormUser user;
            if (tuple.getValueByField("MetricField") instanceof TreeMap) {
                TreeMap<String, OddeeyMetric> MetricList = (TreeMap<String, OddeeyMetric>) tuple.getValueByField("MetricField");
                user = UserList.get(MetricList.firstEntry().getValue().getTags().get("UUID"));
                user.getTmpconsumption().doConsumption(messageprice, MetricList.size());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("SourceComponent " + MetricList.size());
                    LOGGER.debug(user.getEmail() + " " + user.getTmpconsumption().getAmount() + " " + user.getTmpconsumption().getCount());
                }

            }

            if (tuple.getValueByField("MetricField") instanceof OddeeyMetric) {
                user = UserList.get(((OddeeyMetric) tuple.getValueByField("MetricField")).getTags().get("UUID"));
                user.getTmpconsumption().doConsumption(messageprice);
            }
        }
        collector.ack(tuple);
    }
}
