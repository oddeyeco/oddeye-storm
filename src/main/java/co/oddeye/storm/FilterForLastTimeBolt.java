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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class FilterForLastTimeBolt extends BaseRichBolt {

    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(FilterForLastTimeBolt.class);
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metric"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector oc) {
        collector = oc;
    }

    @Override
    public void execute(Tuple input) {
        Object metric = input.getValueByField("metric");
        if (metric instanceof OddeeysSpecialMetric) {            
            collector.emit(new Values(metric));
        }

        if (metric instanceof OddeeyMetric) {
            OddeeyMetric tmp_metric = (OddeeyMetric) metric;
            if (tmp_metric.getName().equals("host_absent"))
            {
                collector.emit(new Values(metric));
            }
        }

        collector.ack(input);
    }

}
