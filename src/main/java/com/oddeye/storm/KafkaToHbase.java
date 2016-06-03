/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.oddeye.storm;


import java.util.UUID;
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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import scala.Option;

/**
 *
 * @author vahan
 */
public class KafkaToHbase extends BaseRichBolt {

    private Table htable = null;
    protected OutputCollector collector;

    private static final Logger logger = Logger.getLogger(KafkaToHbase.class);

    @Override
    public void execute(Tuple input) {

        logger.info("Mtav Bolt: " + input.getString(0));
        Option msgObject = null;
        String msg = input.getString(0);
        java.util.Date date = new java.util.Date();
        byte[] id = Bytes.toBytes(date.toString()+"-"+UUID.randomUUID().toString());
        Put row = new Put(id, date.getTime());
        row.addColumn(Bytes.toBytes("d"), Bytes.toBytes("msg"), Bytes.toBytes(msg));         
        try {
          this.htable.put(row);    
          this.collector.ack(input);
        } catch (Exception e) {
            
            this.collector.reportError(e);
        }
        

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(java.util.Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        Configuration config = HBaseConfiguration.create();
        config.clear();
        config.set("hbase.zookeeper.quorum", "192.168.10.50");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            HBaseAdmin.checkHBaseAvailable(config);
            Connection connection = ConnectionFactory.createConnection(config);
            this.htable = connection.getTable(TableName.valueOf("oddeyetest"));
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
