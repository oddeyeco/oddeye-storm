/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import co.oddeye.core.OddeeysSpecialMetric;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CheckLastTimeBolt extends BaseRichBolt {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CheckLastTimeBolt.class);
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void execute(Tuple input) {

        if (input.getSourceComponent().equals("FilterForLastTimeBolt")) {
            OddeeyMetric metric = (OddeeyMetric) input.getValueByField("metric");
            if (metric instanceof OddeeysSpecialMetric) {
                LOGGER.warn("OddeeysSpecialMetric: Name:" + metric.getName() + " tags:" + metric.getTags());
            } else {
                LOGGER.info(" Name:" + metric.getName() + " tags:" + metric.getTags());
            }
            // Todo Fix last time
        }
        //ToDo Check last time
        collector.ack(input);
    }

}
