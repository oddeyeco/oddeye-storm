/*
 * BoltUsingMBeanImplMBean.java
 *
 * Created on December 22, 2017, 10:45 AM
 */
package co.oddeye.storm.core;

/**
 * Interface BoltUsingMBeanImplMBean
 *
 * @author vahan
 */
public interface BoltUsingMBeanImplMBean {

    /**
     * Get Bolt Executed Count
     *
     * @return
     */
    public long getExecuteCount();

    /**
     * Get Bolt Execute Time AVG
     *
     * @return
     */
    public double getExecuteTimeMean();

    public double getExecuteTimeGeometricMean();

    public double getExecuteTimeMax();

    public double getExecuteTimeMin();

    public double getStandardDeviation();

}
