/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.globalFunctions;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class TimerSpout extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TimerSpout.class);  

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            LOGGER.warn("outputCollector.start");
            Thread.sleep(60000);
            outputCollector.emit(new Values(System.currentTimeMillis()));
            LOGGER.warn("outputCollector.emit");
        } catch (InterruptedException ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }
    
}
