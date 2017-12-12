/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class MerticListGrouper implements CustomStreamGrouping {

    public static final Logger LOGGER = LoggerFactory.getLogger(MerticListGrouper.class);  
    private List<Integer> tasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        tasks = new ArrayList<>(targetTasks);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> rvalue = new ArrayList<>(values.size());
        OddeeyMetric metric =null;
        for (Object o : values) {
            if (o instanceof TreeMap)
            {
                metric = ((TreeMap<Integer, OddeeyMetric>) o).firstEntry().getValue();                
            }
            if (o instanceof OddeeyMetric)
            {
                metric = (OddeeyMetric) o;
            }            
            if (metric!= null)
            {
                rvalue.add(tasks.get(Math.abs(metric.hashCode()) % tasks.size()));
                LOGGER.info("metric " +metric.getName() +" tags:"+ metric.getTags()+ " values"+rvalue);
            }
            else
            {
                LOGGER.error("Invalid Metric");
            }
        }
       
        return rvalue;
    }

}
