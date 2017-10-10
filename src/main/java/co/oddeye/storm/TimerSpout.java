/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.globalFunctions;
import java.util.Map;
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
    private long interval;
    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TimerSpout.class);  
     
    public TimerSpout (long _interval)
    {
        interval = _interval;
    }
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
            LOGGER.debug("outputCollector.start for interval "+interval);
            Thread.sleep(interval);
            outputCollector.emit(new Values(System.currentTimeMillis()));
            LOGGER.debug("outputCollector.emit for interval "+interval);
        } catch (InterruptedException ex) {
            LOGGER.error(globalFunctions.stackTrace(ex));
        }
    }
    
}
