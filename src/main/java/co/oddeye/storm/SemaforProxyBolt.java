/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SemaforProxyBolt extends BaseRichBolt{
    
    protected OutputCollector collector;
    public static final Logger LOGGER = LoggerFactory.getLogger(SemaforProxyBolt.class);
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("action"));
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
        collector.emit(new Values(tuple.getString(0)));
                
    }
    
}
