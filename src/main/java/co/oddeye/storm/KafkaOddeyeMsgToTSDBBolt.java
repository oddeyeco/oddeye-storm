/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

//import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
import net.spy.memcached.MemcachedClient;
import org.apache.commons.codec.binary.Hex;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

/**
 *
 * @author vahan
 */
public class KafkaOddeyeMsgToTSDBBolt extends BaseRichBolt {

    protected OutputCollector collector;

//    private static final Logger logger = Logger.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    public static final Logger logger = LoggerFactory.getLogger(KafkaOddeyeMsgToTSDBBolt.class);
    private TSDB tsdb = null;

    private JsonParser parser = null;
    private JsonArray jsonResult = null;
    private final java.util.Map<String, Object> conf;
    private org.hbase.async.HBaseClient client;

    private byte[] metatable;
    private HashMap<UUID, Set<String>> metricsmap = new HashMap<UUID, Set<String>>();
    private HashMap<UUID, Set<String>> tagksmap = new HashMap<UUID, Set<String>>();
    private HashMap<UUID, Set<String>> tagvsmap = new HashMap<UUID, Set<String>>();
    private int p_weight;
    private String alert_level;
    private double value;
    private int weight;
    private int houre;
    private MemcachedClient t_cache;

    public void updateCache(final Calendar CalendarObj) {

        final int l_houre = CalendarObj.get(Calendar.HOUR_OF_DAY);
        for (int D = 0; D < 7; D++) {
            try {
                //        for (int H = 1; H < 5 + 1; H++) {
                CalendarObj.add(Calendar.DATE, -1);
//                System.out.println();
                logger.info("Update Time" + CalendarObj.getTime().toString());
                final byte[] key = ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(l_houre).array();
                GetRequest request = new GetRequest("oddeyerules", key);
                final ArrayList<KeyValue> hourevalues = client.get(request).joinUninterruptibly();
                for (KeyValue hourevalue : hourevalues) {
                    t_cache.set(Hex.encodeHexString(hourevalue.family()) + Hex.encodeHexString(key) + Hex.encodeHexString(hourevalue.qualifier()), 3600 * 4, ByteBuffer.wrap(hourevalue.value()).getDouble());
                }
            } catch (Exception ex) {
                logger.error(ex.toString());
            }

        }
        t_cache.set(Integer.toString(houre), 3600 * 4, true);
    }

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
        logger.debug("Start KafkaOddeyeMsgToTSDBBolt " + msg);
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
        java.util.Date date = new java.util.Date();
        logger.info("Bolt ready to write to TSDB " + df.format(date.getTime()));
        JsonElement Metric;
        try {
            this.jsonResult = (JsonArray) this.parser.parse(msg);
        } catch (Exception ex) {
            logger.info("msg parse Exception" + ex.toString());
        }
        HashMap<String, String> tags = new HashMap<String, String>();
        UUID uuid;
        Set<String> metriclist;
        Set<String> tagkslist;
        Set<String> tagvslist;
        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    logger.info("Ready count: " + this.jsonResult.size());
                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        if (Metric.getAsJsonObject().get("tags") != null) {
                            tags = gson.fromJson(Metric.getAsJsonObject().get("tags").getAsJsonObject(), tags.getClass());
                            logger.info("tags count: " + tags.size());
                            CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                            logger.info("Metric Time: " + CalendarObj.getTime().toString());
                            uuid = UUID.fromString(tags.get("UUID"));

                            if (tags.get("alert_level") != null) {
                                alert_level = tags.get("alert_level");
                                p_weight = Integer.parseInt(alert_level);
                            } else {
                                alert_level = null;
                            }

                            if ((alert_level == null) || (p_weight < -1)) {
                                weight = 0;
                                houre = CalendarObj.get(Calendar.HOUR_OF_DAY);
//                                if (t_cache.get(Integer.toString(houre)) == null) {
//                                    logger.info("Update data to " + houre);
//                                    updateCache(CalendarObj);                                    
//                                }
                                value = Metric.getAsJsonObject().get("value").getAsDouble();
                            }

                            tagkslist = (Set<String>) tagksmap.get(uuid);
                            if (tagkslist == null) {
                                tagkslist = new HashSet<String>();
                            }
                            tagvslist = (Set<String>) tagvsmap.get(uuid);
                            if (tagvslist == null) {
                                tagvslist = new HashSet<String>();
                            }
                            for (HashMap.Entry<String, String> entry : tags.entrySet()) {
                                if (!tagkslist.contains(entry.getKey())) {
                                    final PutRequest putkey = new PutRequest(this.metatable, uuid.toString().getBytes(), "tagks".getBytes(), entry.getKey().getBytes(), Bytes.fromLong(System.currentTimeMillis()));
                                    this.client.put(putkey);
                                    tagkslist.add(entry.getKey());
                                    tagksmap.put(uuid, tagkslist);
                                }

                                if (!tagvslist.contains((entry.getKey() + "/" + entry.getValue()))) {
                                    final PutRequest putvalue = new PutRequest(this.metatable, uuid.toString().getBytes(), "tagvs".getBytes(), (entry.getKey() + "/" + entry.getValue()).getBytes(), Bytes.fromLong(System.currentTimeMillis()));
                                    this.client.put(putvalue);
                                    tagvslist.add((entry.getKey() + "/" + entry.getValue()));
                                    tagvsmap.put(uuid, tagvslist);
                                }

                            }

                            metriclist = (Set<String>) metricsmap.get(uuid);
                            if (metriclist == null) {
                                metriclist = new HashSet<String>();
                            }
                            if (!metriclist.contains(Metric.getAsJsonObject().get("metric").getAsString())) {
                                final PutRequest putmetric = new PutRequest(this.metatable, uuid.toString().getBytes(), "metrics".getBytes(), Metric.getAsJsonObject().get("metric").getAsString().getBytes(), Bytes.fromLong(System.currentTimeMillis()));
                                this.client.put(putmetric);
                                metriclist.add(Metric.getAsJsonObject().get("metric").getAsString());
                                metricsmap.put(uuid, metriclist);
                            }

                            tsdb.addPoint(Metric.getAsJsonObject().get("metric").getAsString(), Metric.getAsJsonObject().get("timestamp").getAsLong(), Metric.getAsJsonObject().get("value").getAsDouble(), tags);
                            tags.clear();
                        }
                    }
                    this.client.flush();
                    logger.info(this.jsonResult.size() + " Metrics Write to dbase");
                }
            } catch (Exception ex) {
                logger.info("JSON parse Exception" + ex.toString());
            }
        }
        this.collector.ack(input);

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {
        try {
            this.tsdb.shutdown().joinUninterruptibly();
        } catch (Exception ex) {
            logger.error("OpenTSDB shutdown execption : " + ex.toString());
            throw new RuntimeException(ex);
        }
    }

    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        logger.info("DoPrepare KafkaOddeyeMsgToTSDBBolt");
        this.collector = collector;
        this.parser = new JsonParser();

        try {
            t_cache = new MemcachedClient(
                    new InetSocketAddress("192.168.10.60", 11211));
            //TODO do config
            String quorum = String.valueOf(conf.get("zkHosts"));
            Config openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();
            this.client = new org.hbase.async.HBaseClient(quorum);
            this.tsdb = new TSDB(
                    this.client,
                    openTsdbConfig);

        } catch (IOException ex) {
            logger.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            logger.error("OpenTSDB config execption : " + ex.toString());
        }

        logger.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");

    }
}
