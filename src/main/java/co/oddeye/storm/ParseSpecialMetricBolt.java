/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.storm.core.BaseParseMetricBolt;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class ParseSpecialMetricBolt extends BaseParseMetricBolt {    
    public static final Logger LOGGER = LoggerFactory.getLogger(ParseSpecialMetricBolt.class);
    
    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        super.prepare(map, tc, oc);
        LOGGER.warn("DoPrepare ParseMetricBolt");        
        special = true;
    }

}