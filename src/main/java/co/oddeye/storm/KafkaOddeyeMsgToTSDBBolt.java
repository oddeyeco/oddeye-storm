/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

//import com.fasterxml.jackson.databind.ObjectMapper;
import co.oddeye.cache.CacheItem;
import co.oddeye.cache.CacheItemsList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import java.util.Set;
import java.util.UUID;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
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
//import net.spy.memcached.MemcachedClient;
import org.hbase.async.Bytes;
import org.hbase.async.PutRequest;

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

        UUID uuid;
        Set<String> metriclist;
        Set<String> tagkslist;
        Set<String> tagvslist;
        if (this.jsonResult != null) {
            try {
                if (this.jsonResult.size() > 0) {
                    LOGGER.info("Ready count: " + this.jsonResult.size());
                    Metric = this.jsonResult.get(0);
                    tags = gson.fromJson(Metric.getAsJsonObject().get("tags").getAsJsonObject(), tags.getClass());
                    LOGGER.info("tags count: " + tags.size());
                    CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                    LOGGER.info("Metric Time: " + CalendarObj.getTime().toString());
                    uuid = UUID.fromString(tags.get("UUID"));

                    for (int i = 0; i < this.jsonResult.size(); i++) {
                        Metric = this.jsonResult.get(i);
                        if (Metric.getAsJsonObject().get("tags") != null) {
                            tagsjson = gson.fromJson(Metric.getAsJsonObject().get("tags").getAsJsonObject(), tagsjson.getClass());
                            tags = gson.fromJson(Metric.getAsJsonObject().get("tags").getAsJsonObject(), tags.getClass());
                            for (Map.Entry<String, Object> tag : tagsjson.entrySet()) {
                                if (tag.getValue().getClass().equals(String.class)) {
                                    tags.put(tag.getKey(), (String) tag.getValue());
                                }
                                if (tag.getValue().getClass().equals(Double.class)) {
                                    tags.put(tag.getKey(), Long.toString(Math.round((Double) tag.getValue())));
                                }
                            }

                            if (tags.get("alert_level") != null) {
                                alert_level = tags.get("alert_level");
                                p_weight = Integer.parseInt(alert_level);
                            } else {
                                alert_level = null;
                            }

                            if ((alert_level == null) || ((p_weight < 1) && (p_weight > -3))) {
                                weight = 0;
                                houre = CalendarObj.get(Calendar.HOUR_OF_DAY);
                                CalendarObj.setTimeInMillis(Metric.getAsJsonObject().get("timestamp").getAsLong() * 1000);
                                LOGGER.info(CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                value = Metric.getAsJsonObject().get("value").getAsDouble();

                                b_metric = tsdb.getUID(UniqueId.UniqueIdType.METRIC, Metric.getAsJsonObject().get("metric").getAsString());
                                b_UUID = tsdb.getUID(UniqueId.UniqueIdType.TAGV, Metric.getAsJsonObject().get("tags").getAsJsonObject().get("UUID").getAsString());
                                b_host = tsdb.getUID(UniqueId.UniqueIdType.TAGV, Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());

                                qualifier = ArrayUtils.addAll(b_metric, b_UUID);
                                qualifier = ArrayUtils.addAll(qualifier, b_host);

                                for (int j = 0; j < 7; j++) {
                                    CalendarObj.add(Calendar.DATE, -1);
                                    key = ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                                    if (ItemsList.sizebykey(key) == 0) {
                                        ItemsList.addObject(oddeyerulestable, key, client);

                                    }
                                    CacheItem Item = ItemsList.get(key, qualifier);
                                    if (Item == null) {
                                        // Get calculated data 
                                        ItemsList.addObject(oddeyerulestable, key, qualifier, client);
                                        Item = ItemsList.get(key, qualifier);
                                    }

                                    if (Item == null) {
                                        ItemsList.addObject(oddeyerulestable, key, CalendarObj, Metric, client, tsdb);
                                        Item = ItemsList.get(key, qualifier);
                                    }

                                    if (Item == null) {
                                        Item = new CacheItem(key, qualifier);
                                        ItemsList.addObject(Item);
                                        LOGGER.warn("No rule for check: " + CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                        continue;
                                    }
                                    if (p_weight != -1) {
                                        if (Item.getAvg() != null && Item.getDev() != null) {
                                            if (value > Item.getAvg() + Item.getDev()) {
                                                weight++;
                                            }
                                        }
                                        if (Item.getMax() != null) {
                                            if (value > Item.getMax()) {
                                                weight++;
                                            }
                                        }
                                    } else {
                                        LOGGER.info("Check Up Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                    }

                                    if (p_weight != -2) {
                                        if (Item.getMin() != null) {
                                            if (value < Item.getMin()) {
                                                weight++;
                                            }
                                        }
                                        if (Item.getAvg() != null && Item.getDev() != null) {
                                            if (value < Item.getAvg() - Item.getDev()) {
                                                weight++;
                                            }
                                        }
                                    } else {
                                        LOGGER.info("Check Down Disabled : Withs weight" + p_weight + " " + CalendarObj.getTime() + "-" + Metric.getAsJsonObject().get("metric").getAsString() + " " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
                                    }

                                    //To do calculate
                                }
                                
                                tags.put("alert_level", Integer.toString(weight));
                            }

                            tagkslist = tagksmap.get(uuid);
                            if (tagkslist == null) {
                                tagkslist = new HashSet<>();
                            }
                            tagvslist = tagvsmap.get(uuid);
                            if (tagvslist == null) {
                                tagvslist = new HashSet<>();
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

                            metriclist = metricsmap.get(uuid);
                            if (metriclist == null) {
                                metriclist = new HashSet<>();
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
                    LOGGER.info(this.jsonResult.size() + " Metrics Write to dbase");
                    this.collector.ack(input);
                }
            } catch (JsonSyntaxException ex) {
                LOGGER.error("JsonSyntaxException: " + stackTrace(ex));
                this.collector.fail(input);
            } catch (NumberFormatException ex) {
                LOGGER.error("NumberFormatException: " + stackTrace(ex));
                this.collector.fail(input);
            } 
            catch (Exception ex) {
                LOGGER.error("Exception: " + stackTrace(ex));
                this.collector.fail(input);
            }
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
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt");
        this.collector = collector;
        this.parser = new JsonParser();
        ItemsList = new CacheItemsList();

        try {
//            t_cache = new MemcachedClient(
//                    new InetSocketAddress("192.168.10.60", 11211));
            //TODO do config
            String quorum = String.valueOf(conf.get("zkHosts"));
            Config openTsdbConfig = new net.opentsdb.utils.Config(true);
            openTsdbConfig.overrideConfig("tsd.core.auto_create_metrics", String.valueOf(conf.get("tsd.core.auto_create_metrics")));
            openTsdbConfig.overrideConfig("tsd.storage.enable_compaction", String.valueOf(conf.get("tsd.storage.enable_compaction")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.data_table", String.valueOf(conf.get("tsd.storage.hbase.data_table")));
            openTsdbConfig.overrideConfig("tsd.storage.hbase.uid_table", String.valueOf(conf.get("tsd.storage.hbase.uid_table")));

            this.metatable = String.valueOf(conf.get("metatable")).getBytes();
            
            oddeyerulestable = String.valueOf(conf.get("oddeyerulestable"));

            org.hbase.async.Config clientconf = new org.hbase.async.Config();
            clientconf.overrideConfig("hbase.zookeeper.quorum", quorum);
            clientconf.overrideConfig("hbase.rpcs.batch.size", "2048");

            this.client = new org.hbase.async.HBaseClient(clientconf);
            this.tsdb = new TSDB(
                    this.client,
                    openTsdbConfig);

        } catch (IOException ex) {
            LOGGER.error("OpenTSDB config execption : should not be here !!!");
        } catch (Exception ex) {
            LOGGER.error("OpenTSDB config execption : " + ex.toString());
        }
        LOGGER.info("DoPrepare KafkaOddeyeMsgToTSDBBolt Finish");

    }
}
