/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetricMeta;
import java.util.Calendar;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class WarningProcessingBolt extends BaseRichBolt {

    private final java.util.Map<String, Object> conf;
    public static final Logger LOGGER = LoggerFactory.getLogger(WarningProcessingBolt.class);

    private byte[] errortable;
    private final byte[] error_family = "d".getBytes();
    private OutputCollector collector;
    private OddeeyMetricMeta metric;
    private Short weight;
    private Calendar calendarObj;

    public WarningProcessingBolt(java.util.Map config) {
        this.conf = config;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector i_collector) {
        this.errortable = String.valueOf(conf.get("errorstable")).getBytes();
        collector = i_collector;
    }

    @Override
    public void execute(Tuple input) {
        metric =(OddeeyMetricMeta) input.getValueByField("metric");
        weight = input.getShortByField("weight");
        calendarObj =(Calendar) input.getValueByField("calendar");
        LOGGER.info("Strat warning bolt input weight = " + weight+ " Date: "+calendarObj.getTime()+" metric Name:" + metric.getName() + " tags:"+ metric.getTags());        
        
        this.collector.ack(input);
    }

}
