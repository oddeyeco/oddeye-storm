/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

//import com.fasterxml.jackson.databind.ObjectMapper;
import co.oddeye.cache.CacheItem;
import co.oddeye.cache.CacheItemsList;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeyMetricMetaList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import java.util.Set;
import java.util.UUID;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import net.opentsdb.utils.Config;
import org.apache.commons.lang.ArrayUtils;
import org.hbase.async.PutRequest;
//import net.spy.memcached.MemcachedClient;

/**
 *
 * @author vahan
 */
public class KafkaOddeyeMsgToTSDBBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger LOGGER = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    private TSDB tsdb = null;

    private JsonParser parser = null;
    private JsonArray jsonResult = null;
    private final java.util.Map<String, Object> conf;
    private org.hbase.async.HBaseClient client;

    private byte[] metatable;
    private final HashMap<UUID, Set<String>> metricsmap = new HashMap<>();
    private final HashMap<UUID, Set<String>> tagksmap = new HashMap<>();
    private final HashMap<UUID, Set<String>> tagvsmap = new HashMap<>();
    private int p_weight;
    private String alert_level;
    private double value;
    private int weight;
    private int houre;
    private byte[] key;
    private CacheItemsList ItemsList;
    private byte[] b_metric;
    private byte[] b_UUID;
    private byte[] b_host;
    private byte[] qualifier;
    private String oddeyerulestable;
    private String s_metriq;
    private org.hbase.async.Config clientconf;
    private Config openTsdbConfig;
    private int mb;
    private Runtime runtime;
    private CacheItem Item;
    private Date basedate;
    private OddeeyMetricMeta mtrsc;
    private OddeeyMetricMetaList mtrscList;
    private final byte[] family = "d".getBytes();
    private double d_value;

    /**
     *
     * @param config
     */
    public KafkaOddeyeMsgToTSDBBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void execute(Tuple input) {
        final Calendar CalendarObj = Calendar.getInstance();
        Gson gson = new Gson();
        String msg = input.getString(0);
        LOGGER.debug("Start KafkaOddeyeMsgToTSDBBolt " + msg);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        java.util.Date date = new java.util.Date();
        LOGGER.info("Bolt ready to write to TSDB " + df.format(date.getTime()));
        JsonElement Metric;
        try {
            this.jsonResult = (JsonArray) this.parser.parse(msg);
        } catch (Exception ex) {
            LOGGER.info("msg parse Exception" + ex.toString());
        }
        HashMap<String, String> tags = new HashMap<>();
        HashMap<String, Object> tagsjson = new HashMap<>();
        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    LOGGER.debug("Ready count: " + this.jsonResult.size());
                    Metric = this.jsonResult.get(0);
                    CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                    LOGGER.info("Metric Time: " + CalendarObj.getTime().toString());
                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                        mtrsc = new OddeeyMetricMeta(Metric, tsdb);
                        if (!mtrscList.contains(mtrsc)) {
                            mtrscList.add(mtrsc);
                            key = mtrsc.getKey();
                            PutRequest putvalue = new PutRequest(metatable, key, family, "n".getBytes(), key);
                            client.put(putvalue);
                            LOGGER.info("Add metric Meta:" + mtrsc.getName());
                        }
                        mtrsc.getTags().entrySet().stream().forEach((tag) -> {
                            tags.put(tag.getKey(), tag.getValue().getValue());
                        });
                        d_value = Metric.getAsJsonObject().get("value").getAsDouble();
                        tsdb.addPoint(mtrsc.getName(), CalendarObj.getTimeInMillis(), d_value, tags);
                        this.collector.ack(input);
                        LOGGER.debug("Add metric Value:" + mtrsc.getName());                        
                    }
                    LOGGER.debug("metric cache size:" + mtrscList.size());

                }
            } catch (JsonSyntaxException ex) {
                LOGGER.error("JsonSyntaxException: " + stackTrace(ex));
                this.collector.fail(input);
            } catch (NumberFormatException ex) {
                LOGGER.error("NumberFormatException: " + stackTrace(ex));
                this.collector.fail(input);
            } catch (Exception ex) {
                LOGGER.error("Exception: " + stackTrace(ex));
                this.collector.fail(input);
            }
            this.jsonResult = null;
        }
    }

    private String stackTrace(Exception cause) {
        if (cause == null) {
            return "-/-";
        }
        StringWriter sw = new StringWriter(1024);
        final PrintWriter pw = new PrintWriter(sw);
        cause.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {
        try {
            this.tsdb.shutdown().joinUninterruptibly();
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB shutdown execption : " + ex.toString());
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector i_collector) {
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt");
        collector = i_collector;
        parser = new JsonParser();
        ItemsList = new CacheItemsList();

        mb = 1024 * 1024;

        //Getting the runtime reference from system
        runtime = Runtime.getRuntime();

        try {
//            t_cache = new MemcachedClient(
//                    new InetSocketAddress("192.168.10.60", 11211));
            //TODO do config
            String quorum = String.valueOf(conf.get("zkHosts"));
            openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();

            clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");

            while (this.tsdb == null) {
                try {
                    this.client = new org.hbase.async.HBaseClient(clientconf);
                    this.tsdb = new TSDB(
                            this.client,
                            openTsdbConfig);
                } catch (Exception e) {
                    LOGGER.warn("OpenTSDB Connection fail in prepare");
                    LOGGER.error("Exception: " + stackTrace(e));
                }

            }

            try {
                mtrscList = new OddeeyMetricMetaList(tsdb, this.metatable);
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
}
