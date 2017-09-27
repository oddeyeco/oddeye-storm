/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm;

import co.oddeye.core.OddeeyMetric;
import java.util.ArrayList;
import java.util.List;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
@Deprecated
public class MerticGrouper implements CustomStreamGrouping {

    public static final Logger LOGGER = LoggerFactory.getLogger(MerticGrouper.class);  
    private List<Integer> tasks;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        tasks = new ArrayList<>(targetTasks);
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> rvalue = new ArrayList<>(values.size());
        values.stream().map((o) -> (OddeeyMetric) o).map((metric) -> {
            rvalue.add(tasks.get(Math.abs(metric.getTags().hashCode()) % tasks.size()));
            return metric;
        }).forEachOrdered((OddeeyMetric metric) -> {
            LOGGER.info("metric" +metric.getName() +" tags:"+ metric.getTags()+ " values"+rvalue);
        });

        
        return rvalue;
    }

}
