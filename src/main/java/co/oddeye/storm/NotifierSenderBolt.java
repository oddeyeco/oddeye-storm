/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.ErrorState;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.globalFunctions;
import co.oddeye.storm.core.SendToEmail;
import co.oddeye.storm.core.SendToTelegram;
import co.oddeye.storm.core.StormUser;
import com.google.common.reflect.TypeToken;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class NotifierSenderBolt extends BaseRichBolt {

    private final Map conf;
    public static final Logger LOGGER = LoggerFactory.getLogger(NotifierSenderBolt.class);
    private OutputCollector collector;
    private Config openTsdbConfig;
    private org.hbase.async.Config clientconf;
    private byte[] userstable;
    private JsonParser parser;
    private Map<String, StormUser> UserList;
    private Map<String, Object> mailconf;
    

    public NotifierSenderBolt(java.util.Map config) {
        this.conf = config;
    }

    NotifierSenderBolt(Map<String, Object> TSDBconfig, Map<String, Object> Mailconfig) {
        this.conf = TSDBconfig;
        this.mailconf = Mailconfig;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("metric"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try {
            parser = new JsonParser();
            UserList = new HashMap<>();
            LOGGER.warn("DoPrepare NotifierSenderBolt");
            collector = oc;
            String quorum = String.valueOf(conf.get("zkHosts"));
            LOGGER.warn("quorum: " + quorum);
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
            this.userstable = String.valueOf(conf.get("usertable")).getBytes();
            final Scanner user_scanner = globalFunctions.getSecindaryclient(clientconf).newScanner(userstable);
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = user_scanner.nextRows(1000).joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    final StormUser User = new StormUser(row, parser);
                    User.UpdateOptionsList(String.valueOf(conf.get("optionstable")).getBytes(), globalFunctions.getSecindaryclient(clientconf));
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
        try {
            if (tuple.getSourceComponent().equals("kafkaSemaphoreSpot")) {
                JsonObject jsonResult = this.parser.parse(tuple.getString(0)).getAsJsonObject();
                if (jsonResult.get("action").getAsString().equals("changeoptions")) {
                    String filter = jsonResult.get("options").getAsString();
                    UserList.get(jsonResult.get("UUID").getAsString()).getOptionsList().put(jsonResult.get("optionsname").getAsString(), filter);
//                    Map<String, String> map = new HashMap<>();
//                    map = (Map<String, String>) globalFunctions.getGson().fromJson(filter, map.getClass());
//                    UserList.get(jsonResult.get("UUID").getAsString()).getFiltertemplateMap().put(jsonResult.get("filtername").getAsString(), map);
                }
            }
            if (tuple.getSourceComponent().equals("TimeSpout2x")) {
                try {
                    ExecutorService Senderexecutor = Executors.newFixedThreadPool(4);
                    for (Map.Entry<String, StormUser> user : UserList.entrySet()) {
                        if (user.getValue().getErrorsList().size() > 0) {
                            if (LOGGER.isInfoEnabled()) {
                                LOGGER.info("USER: " + user.getValue().getEmail() + " " + user.getValue().getErrorsList().size());
                            }

                            for (Map.Entry<String, Map<String, OddeeySenderMetricMetaList>> sendertype : user.getValue().getErrorsList().entrySet()) {
                                if (sendertype.getValue().size() > 0) {
                                    for (Map.Entry<String, OddeeySenderMetricMetaList> sendertarget : sendertype.getValue().entrySet()) {

                                        if (sendertarget.getValue().size() > 0) {
                                            Runnable Sender = null;
                                            if (sendertype.getKey().equals("telegram")) {
                                                Sender = new SendToTelegram(sendertarget.getValue(), user);
                                            }
                                            if (sendertype.getKey().equals("email")) {
                                                Sender = new SendToEmail(sendertarget.getValue(), user, mailconf);
                                            }
                                            Senderexecutor.submit(Sender);
                                        }
                                    }
                                }
                            }

                        }

                    }

                } catch (Exception ex) {
                    LOGGER.error(globalFunctions.stackTrace(ex));

                }
            }

            if (tuple.getSourceComponent().equals("CompareBolt") || tuple.getSourceComponent().equals("CheckSpecialErrorBolt")) {

                OddeeyMetricMeta metricMeta = (OddeeyMetricMeta) tuple.getValueByField("mtrsc");
                if (metricMeta.getErrorState().getState() != ErrorState.ALERT_STATE_CONT) {
                    final StormUser User = UserList.get(metricMeta.getTags().get("UUID").getValue());
                    for (Map.Entry<String, String> option : User.getOptionsList().entrySet()) {
                        JsonElement optionJSON = parser.parse(option.getValue());
                        if (optionJSON.getAsJsonObject().has("notifier-v")) {
                            if (NotifierSenderBolt.checkOpton(optionJSON.getAsJsonObject(), metricMeta)) {
                                JsonObject notifierjsons = optionJSON.getAsJsonObject().get("notifier-v").getAsJsonObject();
                                for (Map.Entry<String, JsonElement> notifierjson : notifierjsons.entrySet()) {
                                    if (!User.getErrorsList().containsKey(notifierjson.getKey())) {
                                        User.getErrorsList().put(notifierjson.getKey(), new HashMap<>());
                                    }
                                    for (JsonElement notifierjsonitem : notifierjson.getValue().getAsJsonArray()) {
                                        if (!User.getErrorsList().get(notifierjson.getKey()).containsKey(notifierjsonitem.getAsString())) {
                                            User.getErrorsList().get(notifierjson.getKey()).put(notifierjsonitem.getAsString(), new OddeeySenderMetricMetaList(notifierjson.getKey(), notifierjsonitem.getAsString()));
                                        }
                                        try {
                                            User.getErrorsList().get(notifierjson.getKey()).get(notifierjsonitem.getAsString()).put(metricMeta.hashCode(), (OddeeyMetricMeta) metricMeta.clone());
                                            if (LOGGER.isInfoEnabled()) {
                                                LOGGER.info("Send to " + notifierjson.getKey() + "--" + notifierjsonitem.getAsString());
                                            }
                                        } catch (CloneNotSupportedException ex) {
                                            LOGGER.error(globalFunctions.stackTrace(ex));
                                        }

                                    }
                                }

                            }
                        }
                    }

                }
            }
        } catch (JsonSyntaxException e) {
            LOGGER.error(globalFunctions.stackTrace(e));
        }

        collector.ack(tuple);
    }

    public static boolean checkOpton(JsonObject filter, OddeeyMetricMeta metricMeta) {
        boolean filtred = true;

        for (Map.Entry<String, JsonElement> filterentry : filter.get("v").getAsJsonObject().entrySet()) {
            String filterindex = filterentry.getKey();
            if ((filterindex.equals("allfilter"))
                    | (("mlfilter".equals(filterindex)) & (!metricMeta.isSpecial()))
                    | (("manualfilter".equals(filterindex)) & (metricMeta.isSpecial()))) {
                JsonObject filtervalue = filterentry.getValue().getAsJsonObject();
                java.lang.reflect.Type type = new TypeToken<List<String>>() {
                    private static final long serialVersionUID = 7894655478L;
                }.getType();
                for (Map.Entry<String, JsonElement> filterValueentry : filtervalue.entrySet()) {
                    JsonElement filterOPvalue = filterValueentry.getValue();
                    String filterOPop = filter.get("op").getAsJsonObject().get(filterindex).getAsJsonObject().get(filterValueentry.getKey()).getAsString();
                    String key = filterValueentry.getKey();
//                    key.replace("info.", key)
//                    String[] path = key.split("\\.");
//                    OddeeyMetricMeta tmpvalue = metricMeta;
                    String prevlevel = Integer.toString(metricMeta.getErrorState().getPrevlevel());
                    int i_level = metricMeta.getErrorState().getPrevlevel();
                    int level = metricMeta.getErrorState().getLevel();
                    String value = "";
                    if (key.equals("level")) {
                        value = Integer.toString(metricMeta.getErrorState().getLevel());
                    }
                    if (key.contains("info.")) {
                        key = key.replace("info.", "");
                        if (key.equals("name")) {
                            value = metricMeta.getName();
                        }
                        if (key.contains("tags.")) {
                            key = key.replace("tags.", "");
                            String[] path = key.split("\\.");
                            value = metricMeta.getTags().get(path[0]).getValue();
                        }
                    }
                    if (null != filterOPop) {
                        switch (filterOPop) {
                            case "=": {
                                filtred = false;
                                List<String> filterlist = globalFunctions.getGson().fromJson(filterOPvalue.getAsJsonArray().toString(), type);
                                if (filterlist.contains(value)) {
                                    filtred = true;
                                } else {
                                    if (filterlist.contains(prevlevel)) {
                                        filtred = true;
                                    }
                                }
                                break;
                            }
                            case "!": {
                                filtred = false;
                                List<String> filterlist = globalFunctions.getGson().fromJson(filterOPvalue.getAsJsonArray().toString(), type);
                                if (!filterlist.contains(value)) {
                                    filtred = true;
                                } else {
                                    if (!filterlist.contains(prevlevel)) {
                                        filtred = true;
                                    }
                                }
                                break;
                            }
                            case "~": {
                                String filterval = filterOPvalue.getAsString();
                                filtred = value.contains(filterval);
                                break;
                            }
                            case "!~": {
                                String filterval = filterOPvalue.getAsString();
                                filtred = !value.contains(filterval);
                                break;
                            }
                            case "==": {
                                String filterval = filterOPvalue.getAsString();
                                filtred = value.equals(filterval);
                                break;
                            }
                            case "!=": {
                                String filterval = filterOPvalue.getAsString();
                                filtred = !value.equals(filterval);
                                break;
                            }
                            default:
                                break;
                        }
                    }
                    if ((!filtred) && (filterindex.equals("allfilter"))) {
                        return filtred;
                    }
                }
            }
        }
        return filtred;
    }

}
