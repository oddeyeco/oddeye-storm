/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import kafka.utils.Json;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.hbase.common.HBaseClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
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
public class KaftaToJsonBolt extends BaseRichBolt {

    private Table htable = null;    
    
    private static final Logger logger = Logger.getLogger(KaftaToJsonBolt.class);
    @Override
    public void execute(Tuple input) {

        logger.info("Mtav Bolt: " + input.getString(0));
        Option msgObject = null;
        String msg = input.getString(0);
        Function1<String, Object> f = new AbstractFunction1<String, Object>() {
            public Object apply(String s) {
                return Double.parseDouble(s);
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
                        logger.info("Json sargec sksuma grel");
                        
                        UUID uuid = UUID.randomUUID();
                        byte[] buuid = Bytes.add(Bytes.toBytes(uuid.getMostSignificantBits()), Bytes.toBytes(uuid.getLeastSignificantBits()));
                        java.util.Date date = new java.util.Date();
                        Put row = new Put(buuid, date.getTime());

                        row.add(Bytes.toBytes("tags"), Bytes.toBytes("UUID"), Bytes.toBytes(JsonMap.get("UUID").get().toString()));

                        Map tagsMap = (Map) JsonMap.get("tags").get();
                        java.util.Map<String, String> javatagsMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(tagsMap).asJava();

                        for (java.util.Map.Entry entry : javatagsMap.entrySet()) {
                            String value = entry.getValue().toString();
                            if (NumberUtils.isNumber(value)) {
                                Double d_Value = Double.parseDouble(value);
                                row.add(Bytes.toBytes("tags"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(d_Value));
                            } else {
                                row.add(Bytes.toBytes("tags"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(value));
                            }

                        }

                        Map dataMap = (Map) JsonMap.get("data").get();

                        java.util.Map<String, String> javadataMap = (java.util.Map<String, String>) JavaConverters$.MODULE$.mapAsJavaMapConverter(dataMap).asJava();

                        for (java.util.Map.Entry entry : javadataMap.entrySet()) {
                            String value = entry.getValue().toString();
                            if (NumberUtils.isNumber(value)) {
                                Double d_Value = Double.parseDouble(value);
                                row.add(Bytes.toBytes("data"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(d_Value));
                            } else {
                                row.add(Bytes.toBytes("data"), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(value));
                            }

                        }
                        this.htable.put(row);
//                        this.htable.flushCommits();
                        logger.info("grec prcav");
                    } catch (Exception e) {
                        logger.error(e);
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
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", "192.168.10.50");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            HBaseAdmin.checkHBaseAvailable(config);                        
            Connection connection = ConnectionFactory.createConnection(config);
            this.htable = connection.getTable(TableName.valueOf("oddeyedata"));
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
