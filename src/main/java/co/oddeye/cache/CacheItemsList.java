/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.uid.UniqueId;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.hbase.async.BinaryComparator;
import org.hbase.async.CompareFilter;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.QualifierFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class CacheItemsList implements Cache<String, CacheItem> {

    static final Logger LOGGER = LoggerFactory.getLogger(CacheItemsList.class);
    private final Cache<String, CacheItem> cache = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterAccess(1, TimeUnit.HOURS).build();
    private final Map<String, Long> Keyscounter = new HashMap<>();



    public long sizebykey(byte[] key) {
        if (null != Keyscounter.get(Hex.encodeHexString(key))) {
            return Keyscounter.get(Hex.encodeHexString(key));
        }
        return 0;
    }

    @Override
    public CacheItem getIfPresent(Object key) {
        return cache.getIfPresent((String) key);
    }

    @Override
    public CacheStats stats() {
        System.out.println(cache.getClass().getName());
        return cache.stats();
    }

    public void addObject(CacheItem Item) {
        final Long counter = sizebykey(Item.getKey());
        cache.put(Item.getIdString(), Item);
        Keyscounter.put(Hex.encodeHexString(Item.getKey()), counter + 1);
    }

    public CacheItem get(String id) {
        return cache.getIfPresent(id);
    }

    public CacheItem get(byte[] id) {
        return cache.getIfPresent(Hex.encodeHexString(id));
    }

    public CacheItem get(byte[] key, byte[] qualifier) {
        final String id = Hex.encodeHexString(ArrayUtils.addAll(key, qualifier));
        return cache.getIfPresent(id);
    }

    public CacheItemsList addObject(String table, byte[] key, byte[] qualifier, HBaseClient client) throws Exception {
        if (table == null) {
            LOGGER.warn("table is null in function line 94");
            throw new Exception("table is null"); 
        }
        if (key == null) {
            LOGGER.warn("key is null in function line 94");
            throw new Exception("key is null"); 
        }
        if (qualifier == null) {
            LOGGER.warn("qualifier is null in function line 94");
            throw new Exception("qualifier is null"); 
        }
        if (client == null) {
            LOGGER.warn("client is null in function line 94");
            throw new Exception("client is null"); 
        }

        final Long counter = sizebykey(key);

        GetRequest request = new GetRequest(table, key);
        QualifierFilter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(qualifier));
        request.setFilter(filter);
        final ArrayList<KeyValue> hourevalues = client.get(request).joinUninterruptibly();
        CacheItem Item;
        for (KeyValue hourevalue : hourevalues) {
            Item = this.get(key, hourevalue.qualifier());
            if (Item == null) {
                Keyscounter.put(Hex.encodeHexString(key), counter + 1);
                Item = new CacheItem(hourevalue);
                LOGGER.warn("Rule for " + Hex.encodeHexString(qualifier) + " key:" + Hex.encodeHexString(qualifier) + " add to cache");
            } else {
                Item.update(hourevalue);
            }
            this.addObject(Item);
        }

        return this;
    }

    public CacheItemsList addObject(String table, byte[] key, HBaseClient client) throws Exception {
        if (table == null) {
            LOGGER.warn("table is null in function line 134");
            throw new Exception("table is null"); 
        }
        if (key == null) {
            LOGGER.warn("key is null in function line 134");
            throw new Exception("key is null"); 
        }
        if (client == null) {
            LOGGER.warn("client is null in function line 134");
            throw new Exception("client is null"); 
        }

        Long counter = sizebykey(key);

        GetRequest request = new GetRequest(table, key);
        final ArrayList<KeyValue> hourevalues = client.get(request).joinUninterruptibly();
        CacheItem Item;
        for (KeyValue hourevalue : hourevalues) {
            Item = this.get(key, hourevalue.qualifier());
            if (Item == null) {
                counter++;
                Item = new CacheItem(hourevalue);
            } else {
                Item.update(hourevalue);
            }
            this.addObject(Item);
        }

        Keyscounter.put(Hex.encodeHexString(key), counter);
        return this;
    }

    public CacheItemsList addObject(String table, byte[] key, Calendar CalendarObj, JsonElement Metric, HBaseClient client, TSDB tsdb) throws Exception {
        if (table == null) {
            LOGGER.warn("table is null in function line 169");
            throw new Exception("table is null"); 
        }
        if (key == null) {
            LOGGER.warn("key is null in function line 169");
            throw new Exception("key is null"); 
        }
        if (CalendarObj == null) {
            LOGGER.warn("CalendarObj is null in function line 169");
            throw new Exception("CalendarObj is null"); 
        }
        if (Metric == null) {
            LOGGER.warn("Metric is null in function line 169");
            throw new Exception("Metric is null"); 
        }
        if (client == null) {
            LOGGER.warn("client is null in function line 169");
            throw new Exception("client is null"); 
        }
        if (tsdb == null) {
            LOGGER.warn("tsdb is null in function line 169");
            throw new Exception("tsdb is null"); 
        }
        

        Long counter = sizebykey(key);
        // Calculate Rule
        Map<String, String> Tagmap;
        byte[] qualifier;
        byte[] family;
        CacheItem Item;
        final TSQuery tsquery = new TSQuery();
        final Calendar StartCalendarObj = Calendar.getInstance();
        byte[] l_key = new byte[12];
        byte[] R_value = new byte[8];

        StartCalendarObj.set(Calendar.MILLISECOND, 0);
        StartCalendarObj.set(Calendar.SECOND, 0);
        StartCalendarObj.set(Calendar.MINUTE, 0);
        StartCalendarObj.set(Calendar.HOUR, CalendarObj.get(Calendar.HOUR));
        StartCalendarObj.set(Calendar.DAY_OF_YEAR, CalendarObj.get(Calendar.DAY_OF_YEAR));
        StartCalendarObj.set(Calendar.YEAR, CalendarObj.get(Calendar.YEAR));

        final Calendar EndCalendarObj = (Calendar) StartCalendarObj.clone();

        EndCalendarObj.add(Calendar.HOUR, 1);
        EndCalendarObj.add(Calendar.MILLISECOND, -1);

        tsquery.setStart(Long.toString(StartCalendarObj.getTimeInMillis()));
        tsquery.setEnd(Long.toString(EndCalendarObj.getTimeInMillis()));
        final List<TagVFilter> filters = new ArrayList<>();
        final ArrayList<TSSubQuery> sub_queries = new ArrayList<>(1);
        final Map<String, String> querytags = new HashMap<>();
        final Map<String, SeekableView> datalist = new TreeMap<>();
        querytags.put("host", Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString());
        querytags.put("UUID", Metric.getAsJsonObject().get("tags").getAsJsonObject().get("UUID").getAsString());
        TagVFilter.mapToFilters(querytags, filters, true);

        final TSSubQuery sub_query_dev = new TSSubQuery();
        String metric = Metric.getAsJsonObject().get("metric").getAsString();
        sub_query_dev.setMetric(metric);
        sub_query_dev.setAggregator("dev");
        sub_query_dev.setFilters(filters);
        sub_query_dev.setDownsample("1h-dev");
        sub_query_dev.setIndex(0);
        sub_queries.add(sub_query_dev);

        final TSSubQuery sub_query_avg = new TSSubQuery();
        sub_query_avg.setMetric(metric);
        sub_query_avg.setAggregator("avg");

        sub_query_avg.setFilters(filters);
        sub_query_avg.setDownsample("1h-avg");
        sub_queries.add(sub_query_avg);

        final TSSubQuery sub_query_max = new TSSubQuery();
        sub_query_max.setMetric(metric);
        sub_query_max.setAggregator("max");

        sub_query_max.setFilters(filters);
        sub_query_max.setDownsample("1h-max");
        sub_queries.add(sub_query_max);
        final TSSubQuery sub_query_min = new TSSubQuery();
        sub_query_min.setMetric(metric);
        sub_query_min.setAggregator("min");

        sub_query_min.setFilters(filters);
        sub_query_min.setDownsample("1h-min");
        sub_queries.add(sub_query_min);
//            break;

//                                    querytags.clear();
        tsquery.setQueries(sub_queries);
        tsquery.validateAndSetQuery();
        Query[] tsdbqueries = tsquery.buildQueries(tsdb);
        // create some arrays for storing the results and the async calls
        final int nqueries = tsdbqueries.length;
        boolean dataadded = false;
        for (int nq = 0; nq < nqueries; nq++) {
            final DataPoints[] series = tsdbqueries[nq].run();
            for (final DataPoints datapoints : series) {
                try {
                    Tagmap = datapoints.getTags();
                } catch (Exception e) {
                    LOGGER.warn("Invalid tags");
                    continue;
                }
                final SeekableView Datalist = datapoints.iterator();
                while (Datalist.hasNext()) {
                    final DataPoint Point = Datalist.next();
                    CalendarObj.setTimeInMillis(Point.timestamp());
                    qualifier = ArrayUtils.addAll(datapoints.metricUID(), tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("UUID")));
                    qualifier = ArrayUtils.addAll(qualifier, tsdb.getUID(UniqueId.UniqueIdType.TAGV, Tagmap.get("host")));
                    family = sub_queries.get(datapoints.getQueryIndex()).downsamplingSpecification().getFunction().toString().getBytes();
                    l_key = ByteBuffer.allocate(12).putInt(CalendarObj.get(Calendar.YEAR)).putInt(CalendarObj.get(Calendar.DAY_OF_YEAR)).putInt(CalendarObj.get(Calendar.HOUR_OF_DAY)).array();
                    R_value = ByteBuffer.allocate(8).putDouble(Point.doubleValue()).array();

                    final PutRequest putrule = new PutRequest(table.getBytes(), l_key, family, qualifier, R_value);

                    Item = this.get(l_key, qualifier);
                    if (Item == null) {
                        Keyscounter.put(Hex.encodeHexString(key), counter + 1);
                        Item = new CacheItem(l_key, qualifier);
                    }

                    Item.update(family, R_value);
                    this.addObject(Item);
                    dataadded = true;
                    client.put(putrule);
                }

            }
        }

        if (dataadded) {
            client.flush();
            LOGGER.warn("add rule for host:" + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString() + " metric:" + metric + " time:" + Hex.encodeHexString(l_key));
        } else {
            LOGGER.warn("Not data for caclculate rule for host:" + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host").getAsString() + " metric:" + metric + " time:" + CalendarObj.getTime());
        }
        return this;
    }

    @Override
    public CacheItem get(String key, Callable<? extends CacheItem> valueLoader) throws ExecutionException {
        return cache.get(key, valueLoader);
    }

    @Override
    public ImmutableMap<String, CacheItem> getAllPresent(Iterable<?> keys) {
        return cache.getAllPresent(keys);

    }

    @Override
    public void put(String key, CacheItem value) {
        cache.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ? extends CacheItem> m) {

        cache.putAll(m);
    }

    @Override
    public void invalidate(Object key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public ConcurrentMap<String, CacheItem> asMap() {
        return cache.asMap();
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public long size() {
        return cache.size();
    }    

}
