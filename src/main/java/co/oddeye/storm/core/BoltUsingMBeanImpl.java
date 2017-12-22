/*
 * BoltUsingMBeanImpl.java
 *
 * Created on December 22, 2017, 10:45 AM
 */
package co.oddeye.storm.core;

import javax.management.*;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Class BoltUsingMBeanImpl
 *
 * @author vahan
 */
public class BoltUsingMBeanImpl implements BoltUsingMBeanImplMBean {

    private final DescriptiveStatistics stats = new DescriptiveStatistics();

    public BoltUsingMBeanImpl() {
    }

    public void AddTimeStats(long time) {
        stats.addValue(time);
    }

    @Override
    public long getExecuteCount() {
        return stats.getN();
    }

    @Override
    public double getExecuteTimeMean() {
        return stats.getMean();
    }

    @Override
    public double getExecuteTimeGeometricMean() {
        return stats.getGeometricMean();
    }    
    @Override
    public double getExecuteTimeMax() {
        return stats.getMax();
    }        
    
    @Override
    public double getExecuteTimeMin() {
        return stats.getMin();
    }            
    
    @Override
    public double getStandardDeviation() {
        return stats.getStandardDeviation();
    }     
}
