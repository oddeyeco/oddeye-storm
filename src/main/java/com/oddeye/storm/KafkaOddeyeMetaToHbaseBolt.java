/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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
public class KafkaOddeyeMetaToHbaseBolt extends BaseRichBolt {

    private Table htable = null;
    protected OutputCollector collector;

    private static final Logger logger = Logger.getLogger(KafkaOddeyeMsgToHbaseBolt.class);

    // Congig parametors
    protected String configKey = "hbase.config";
    protected String tableName;

    @Override
    public void execute(Tuple input) {

        DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS");
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
        Option msgObject = JSON.parseFull(msg);        
        if (!msgObject.isEmpty()) {
            Object maps = msgObject.productElement(0);
            if (maps instanceof Map) {
                Map JsonMap = (Map) maps;
                if (!JsonMap.get("UUID").isEmpty() & !JsonMap.get("tags").isEmpty() & !JsonMap.get("data").isEmpty()) {
                    try {
                        
                        java.util.Date date = new java.util.Date();
                        long startdate = System.currentTimeMillis();
                        logger.info("Meta Ready to write hbase " + df.format(date.getTime()));                        
//                        logger.info("Meta Ready to write hbase");

//                        UUID uuid = UUID.randomUUID();
//                        byte[] buuid = Bytes.add(Bytes.toBytes(uuid.getMostSignificantBits()), Bytes.toBytes(uuid.getLeastSignificantBits()));
//                        java.util.Date date = new java.util.Date();
//                        Put row = new Put(buuid, date.getTime());
//
//                        row.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("UUID"), Bytes.toBytes(JsonMap.get("UUID").get().toString()));

                        String msgUUID = JsonMap.get("UUID").get().toString();
                        Map tagsMap = (Map) JsonMap.get("tags").get();
                        Map dataMap = (Map) JsonMap.get("data").get();

                        java.util.Map<String, String> javatagsMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(tagsMap).asJava();
                        java.util.Map<String, String> javadataMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(dataMap).asJava();

                        ArrayList keysList = new ArrayList();
                        for (java.util.Map.Entry tagentry : javatagsMap.entrySet()) {
                            String tagvalue = tagentry.getValue().toString();
                            String tagkey = tagentry.getKey().toString();
                            if (tagkey.equals("timestamp")) {
                                continue;
                            }
                            for (java.util.Map.Entry keyentry : javadataMap.entrySet()) {
                                String key = keyentry.getKey().toString();
                                String rowkey = msgUUID + "/" + tagkey + "/" + tagvalue + "/" + key;
//                                keysList.add(rowkey);
                                byte[] Browkey = Bytes.toBytes(rowkey);
                                Get g = new Get(Browkey);
                                Result metaRow = this.htable.get(g);
                                if (metaRow.getRow() == null) {                                    
                                    Put row = new Put(Browkey, date.getTime());
                                    row.addColumn(Bytes.toBytes("data"), Bytes.toBytes("tagkey"), Bytes.toBytes(tagkey));
                                    row.addColumn(Bytes.toBytes("data"), Bytes.toBytes("tagvalue"), Bytes.toBytes(tagvalue));
                                    row.addColumn(Bytes.toBytes("data"), Bytes.toBytes("datakey"), Bytes.toBytes(key));
                                    this.htable.put(row);
                                    long enddate = System.currentTimeMillis() - startdate;
                                    logger.info("Meta Writing Finish "+enddate+"ms "+df.format(date.getTime()));
                                }

                            }

                        }
                        logger.info("Meta is exist");
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
        this.tableName = "oddeyemeta";
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
        logger.info(conf.get("metatablename"));

        if (conf.get("metatablename") != null) {
            this.tableName = String.valueOf(conf.get("metatablename"));
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
