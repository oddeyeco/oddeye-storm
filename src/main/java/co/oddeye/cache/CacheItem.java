/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
public class CacheItem {

    private Double min;
    private Double max;
    private Double avg;
    private Double dev;
    private final byte[] id;
    private final byte[] key;
    private final byte[] qualifier;

    
    public CacheItem(byte[] p_key,byte[] p_qualifier) {

        key = p_key; //ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(houre).array();
        qualifier = p_qualifier; //qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID"))); //qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));   
        
        id = ArrayUtils.addAll(key,qualifier);
    }    
    
    public CacheItem(KeyValue data) {

        if (Arrays.equals("min".getBytes(), data.family())) {
            min = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("max".getBytes(), data.family())) {
            max = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), data.family())) {
            avg = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), data.family())) {
            dev = ByteBuffer.wrap(data.value()).getDouble();
        }

        key = data.key(); //ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(houre).array();
        qualifier = data.qualifier(); //qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID"))); //qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));   
        
        id = ArrayUtils.addAll(key,qualifier);
    }

        
    public CacheItem update(byte[] family,byte[] value) throws Exception {

        if (Arrays.equals("min".getBytes(), family)) {
            min = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("max".getBytes(), family)) {
            max = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), family)) {
            avg = ByteBuffer.wrap(value).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), family)) {
            dev = ByteBuffer.wrap(value).getDouble();
        }

        
    return this;
    }
    public CacheItem update(KeyValue data) throws Exception {

        if (!Arrays.equals(key, data.key())) {
            throw new Exception("Not equals key.");
        }

        if (!Arrays.equals(qualifier, data.qualifier())) {
            throw new Exception("Not equals qualifier.");
        }

        if (Arrays.equals("min".getBytes(), data.family())) {
            min = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("max".getBytes(), data.family())) {
            max = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("avg".getBytes(), data.family())) {
            avg = ByteBuffer.wrap(data.value()).getDouble();
        }
        if (Arrays.equals("dev".getBytes(), data.family())) {
            dev = ByteBuffer.wrap(data.value()).getDouble();
        }

        return this;
    }

    /**
     * @return the min
     */
    public Double getMin() {
        return min;
    }

    /**
     * @return the max
     */
    public Double getMax() {
        return max;
    }

    /**
     * @return the avg
     */
    public Double getAvg() {
        return avg;
    }

    /**
     * @return the dev
     */
    public Double getDev() {
        return dev;
    }

    /**
     * @return the id
     */
    public byte[] getId() {
        return id;
    }

    public String getIdString() {
        return Hex.encodeHexString(id);
    }

    /**
     * @return the key
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * @return the qualifier
     */
    public byte[] getQualifier() {
        return qualifier;
    }

}
