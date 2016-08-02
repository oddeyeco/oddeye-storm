/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import scala.Function1;
import scala.Option;
import scala.collection.JavaConverters$;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction1;
import scala.util.parsing.json.JSON;

/**
 *
 * @author vahan
 */
public class KafkaOddeyeMsgToHbaseBolt extends BaseRichBolt {

    private Table htable = null;
    protected OutputCollector collector;

    private static final Logger logger = Logger.getLogger(KafkaOddeyeMsgToHbaseBolt.class);

    // Congig parametors
    protected String configKey = "hbase.config";
    protected String tableName;

    @Override
    public void execute(Tuple input) {
        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
//        logger.info("Start bolt vs message: " + input.getString(0));
        Option msgObject = null;
        String msg = input.getString(0);
        Function1<String, Object> f = new AbstractFunction1<String, Object>() {
            public Object apply(String s) {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    return Double.parseDouble(s);
                }
            }
        };

        JSON.globalNumberParser_$eq(f);
        msgObject = JSON.parseFull(msg);
        Map JsonMap = null;
        if (!msgObject.isEmpty()) {
            Object maps = msgObject.productElement(0);
            if (maps instanceof Map) {
                JsonMap = (Map) maps;
                if (!JsonMap.get("UUID").isEmpty() & !JsonMap.get("tags").isEmpty() & !JsonMap.get("data").isEmpty()) {
                    try {
                        java.util.Date date = new java.util.Date();
                        long startdate = System.currentTimeMillis();
                        logger.info("Message Ready to write hbase server time " + df.format(date.getTime()));
                        Map tagsMap = (Map) JsonMap.get("tags").get();
                        UUID uuid = uuid = UUID.fromString(JsonMap.get("UUID").get().toString());
                        java.util.Map<String, String> javatagsMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(tagsMap).asJava();
                        String tagvalue;
                        String tagkey;
                        String rowkey = "/";
                        Long clienttimestamp = null;

                        for (java.util.Map.Entry entry : javatagsMap.entrySet()) {
                            tagvalue = entry.getValue().toString();
                            tagkey = entry.getKey().toString();

                            if ((!tagkey.equals("UUID")) & (!tagkey.equals("timestamp"))) {
                                rowkey = rowkey+tagkey + "=" + tagvalue + "/";
                            }
                            if (tagkey.equals("UUID")) {
                                uuid = UUID.fromString(tagvalue);
                            }
                            if (tagkey.equals("timestamp")) {
                                clienttimestamp = Long.parseLong(tagsMap.get("timestamp").get().toString()) * 1000;
                            }

                        }

                        if ((clienttimestamp != null) & (uuid != null)) {
                            byte[] buuid = Bytes.add(Bytes.toBytes(uuid.getMostSignificantBits()), Bytes.toBytes(uuid.getLeastSignificantBits()),Bytes.toBytes(clienttimestamp) );
//                            byte[] buuid = Bytes.add(Bytes.toBytes(uuid.toString()),Bytes.toBytes(rowkey) );
                            byte[] B_rowkey = Bytes.add(buuid, Bytes.toBytes(rowkey) );
                            Put row = new Put(B_rowkey, date.getTime());

                            row.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("UUID"), Bytes.toBytes(JsonMap.get("UUID").get().toString()));

                            for (java.util.Map.Entry entry : javatagsMap.entrySet()) {
                                String value = entry.getValue().toString();
                                if (NumberUtils.isNumber(value)) {
                                    Double d_Value = Double.parseDouble(value);
                                    row.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(d_Value));
                                } else {
                                    row.addColumn(Bytes.toBytes("tags"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(value));
                                }

                            }

                            Map dataMap = (Map) JsonMap.get("data").get();

                            java.util.Map<String, String> javadataMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(dataMap).asJava();

                            for (java.util.Map.Entry entry : javadataMap.entrySet()) {
                                String value = entry.getValue().toString();
                                if (NumberUtils.isNumber(value)) {
                                    Double d_Value = Double.parseDouble(value);
                                    row.addColumn(Bytes.toBytes("data"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(d_Value));
                                } else {
                                    row.addColumn(Bytes.toBytes("data"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(value));
                                }

                            }
                            this.htable.put(row);
                            java.util.Date clientdate = new java.util.Date(Long.parseLong(tagsMap.get("timestamp").get().toString()) * 1000);
                            long enddate = System.currentTimeMillis() - startdate;
                            logger.info("Writing Finish:interval " + enddate + " server time " + df.format(date.getTime()) + " client time " + df.format(clientdate.getTime()));
                        } else {
                            logger.info("Writing falil: clienttimestamp or uuid is null");
                        }
                    } catch (Exception e) {
                        this.collector.reportError(e);
                    }

                } else {
                    logger.error("JSON Not valid");
                }
            } else {
                logger.error("Data Not Mapped");
            }
        } else {
            logger.error("Data Not Json");
        }
        this.collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        logger.info("DoPrepare KaftaOddeyeMsgToHbaseBolt");
        this.tableName = "oddeyedata";
        this.collector = collector;
        Configuration config = HBaseConfiguration.create();
        config.clear();

        java.util.Map<String, Object> conf = (java.util.Map<String, Object>) map.get(this.configKey);

        if (conf == null) {
            throw new IllegalArgumentException("HBase configuration not found using key '" + this.configKey + "'");
        }

        if (conf.get("hbase.rootdir") == null) {
            logger.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
        }

        config.set("hbase.zookeeper.quorum", String.valueOf(conf.get("zookeeper.quorum")));
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(conf.get("zookeeper.clientPort")));
        logger.info(conf.get("datatablename"));

        if (conf.get("datatablename") != null) {
            this.tableName = String.valueOf(conf.get("datatablename"));
        }
        try {
            Connection connection = ConnectionFactory.createConnection(config);
            this.htable = connection.getTable(TableName.valueOf(this.tableName));
            logger.info("Connect to table " + this.tableName);
        } catch (Exception e) {
            logger.error(e);
        }
        if (this.htable == null) {
            throw new RuntimeException("Hbase Table '" + this.tableName + "' Can not connect");
        }
    }
}
