/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.AlertLevel;
import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.OddeyeTag;
import co.oddeye.core.globalFunctions;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class StormUser implements Serializable {

    public class Consumption implements Serializable {

        private Double amount = 0.0;
        private Integer count = 0;
        ReentrantLock lock = new ReentrantLock();

        /**
         * @return the amount
         */
        public Double getAmount() {
            return amount;
        }

        /**
         * @return the count
         */
        public Integer getCount() {
            return count;
        }

        public void doConsumption(Double value, Integer _count) {
            while (lock.isLocked()) {
                System.out.println("isLocked");
            }
            amount = amount + value * _count;
            count = count + _count;
        }

        public void doConsumption(Double value) {
            lock.lock();
            try {
                amount = value;
                count++;

            } finally {
                lock.unlock();
            }
        }

        public void clear() {
            amount = 0.0;
            count = 0;
        }
    }
    public static final Logger LOGGER = LoggerFactory.getLogger(StormUser.class);

    private UUID id;
    private String lastname;
    private String name;
    private String email;
    private String company;
    private String country;
    private String city;
    private String region;
    private String timezone = null;
    private Double balance;
    private Consumption tmpconsumption = new Consumption();
    private final Map<String, String> FiltertemplateList = new HashMap<>();
    private final Map<String, Map<String, String>> FiltertemplateMap = new HashMap<>();

    private final AlertLevel AlertLevels = new AlertLevel();
    private final Map<String, OddeeySenderMetricMetaList> TargetList = new HashMap<>();
//    private final OddeeySenderMetricMetaList TelegramList = new OddeeySenderMetricMetaList();

    public StormUser(ArrayList<KeyValue> row, JsonParser parser) {

        for (KeyValue property : row) {
            if (Arrays.equals(property.qualifier(), "UUID".getBytes())) {
                this.id = UUID.fromString(new String(property.value()));
            }

            if (Arrays.equals(property.qualifier(), "email".getBytes())) {
                this.email = new String(property.value());
            }
            if (Arrays.equals(property.qualifier(), "name".getBytes())) {
                this.name = new String(property.value());
            }
            if (Arrays.equals(property.qualifier(), "lastname".getBytes())) {
                this.lastname = new String(property.value());
            }
            if (Arrays.equals(property.qualifier(), "company".getBytes())) {
                this.company = new String(property.value());
            }
            if (Arrays.equals(property.qualifier(), "country".getBytes())) {
                this.country = new String(property.value());
            }
            if (Arrays.equals(property.qualifier(), "city".getBytes())) {
                this.city = new String(property.value());
            }

            if (Arrays.equals(property.qualifier(), "balance".getBytes())) {
                this.balance = ByteBuffer.wrap(property.value()).getDouble();//Bytes.getLong(property.value());
            }
            if ((Arrays.equals(property.qualifier(), "timezone".getBytes()))) {
                if ((Arrays.equals(property.family(), "personalinfo".getBytes()))) {
                    this.timezone = new String(property.value());
                } else {
                    if (this.timezone == null) {
                        this.timezone = new String(property.value());
                    }
                }
            }
            if (Arrays.equals(property.family(), "filtertemplates".getBytes())) {
                final String filter = new String(property.value());
                final String filtername = new String(property.qualifier());
                this.FiltertemplateList.put(filtername, filter);
                Gson gson = new Gson();
                Map<String, String> map = new HashMap<>();
                map = (Map<String, String>) gson.fromJson(filter, map.getClass());
                FiltertemplateMap.put(filtername, map);

            }

        }

    }

    public void PrepareNotifier(OddeeyMetricMeta metric, OddeeyMetricMeta oldmetric, boolean isStart) {

        boolean filtred = false;
        Map<String, String> NotifiersList = new HashMap<>();
        Map<String, Boolean> RemoveList = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> filterEntry : this.getFiltertemplateMap().entrySet()) {
            String chatid = "";
            final String filtername = filterEntry.getKey();
            final Map<String, String> filter = filterEntry.getValue();
            if ((!filtername.equals("oddeye_base_send_telegram")) && (!filtername.equals("oddeye_base_send_email"))) {
                continue;
            }

            if (filtername.equals("oddeye_base_send_telegram")) {
                if (filter.containsKey("send_telegram")) {
                    if (filter.containsKey("telegram_input")) {
                        chatid = filter.get("telegram_input");
                    }
                    if (filter.get("send_telegram").equals("on") && !chatid.isEmpty()) {
                        if (Checkforfilter(filter, metric)) {
                            NotifiersList.put(filtername, chatid);
                            filtred = true;
                            RemoveList.put(filtername.replace("oddeye_base_send_", ""), false);
                        } else {
                            RemoveList.put(filtername.replace("oddeye_base_send_", ""), true);
                            if (!isStart) {
                                if (Checkforfilter(filter, oldmetric)) {
                                    NotifiersList.put(filtername, chatid);
                                    filtred = true;
                                }
                            }
                        }
                    }
                }
            }

            if (filtername.equals("oddeye_base_send_email")) {
                if (filter.containsKey("send_email")) {
                    chatid = this.email;
                    if (filter.containsKey("email_input")) {
                        if (!filter.get("email_input").isEmpty()) {
                            chatid = filter.get("email_input");
                        }
                    }

                    if (filter.get("send_email").equals("on")) {
                        if (Checkforfilter(filter, metric)) {
                            NotifiersList.put(filtername, chatid);
                            filtred = true;
                            RemoveList.put(filtername.replace("oddeye_base_send_", ""), false);
                        } else {
                            RemoveList.put(filtername.replace("oddeye_base_send_", ""), true);
                            if (!isStart) {
                                if (Checkforfilter(filter, oldmetric)) {
                                    NotifiersList.put(filtername, chatid);
                                    filtred = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (filtred) {
            if (metric != null) {
                for (Map.Entry<String, String> targetEntry : NotifiersList.entrySet()) {
                    String target = targetEntry.getKey();
                    target = target.replace("oddeye_base_send_", "");
                    OddeeySenderMetricMetaList Sendlist;
                    if (!TargetList.containsKey(target)) {
                        getTargetList().put(target, new OddeeySenderMetricMetaList());
                    }

                    Sendlist = getTargetList().get(target);
                    try {
                        if ((RemoveList.get(target)) && (Sendlist.containsKey(metric.hashCode()))) {
                            Sendlist.remove(metric.hashCode());
                        } else {
                            Sendlist.set(metric);
                            Sendlist.setTargetType(target);
                            Sendlist.setTargetValue(targetEntry.getValue());
                        }

                    } catch (Exception ex) {
                        LOGGER.error(globalFunctions.stackTrace(ex));
                    }

                }
            }

        }
    }

    private boolean Checkforfilter(Map<String, String> filter, OddeeyMetricMeta metric) {

        boolean result = false;
        if (metric == null) {
            return false;
        }

        int level = metric.getErrorState().getLevel();
        if (filter.containsKey("check_level_" + level)) {
            if (filter.get("check_level_" + level).equals("on")) {
                result = true;
            }
        }
        if (!result) {
            return result;
        }

        for (Map.Entry<String, OddeyeTag> tag : metric.getTags().entrySet()) {
            if ((filter.containsKey("check_" + tag.getKey()))) {
                if (filter.containsKey(tag.getKey() + "_input")) {
                    Pattern r = Pattern.compile(filter.get(tag.getKey() + "_input"));
                    Matcher m = r.matcher(tag.getValue().getValue());
                    result = m.find();
                    if (!result) {
                        break;
                    }
                }
            }
        }

        if (!result) {
            return result;
        }

        if ((filter.containsKey("check_metric"))) {
            if (filter.containsKey("metric_input")) {
                Pattern r = Pattern.compile(filter.get("metric_input"));
                Matcher m = r.matcher(metric.getName());
                result = m.find();
            }
        }

//        LOGGER.warn(result+" Level "+metric.getErrorState().getLevel());
        return result;
    }

    /**
     * @return the id
     */
    public UUID getId() {
        return id;
    }

    /**
     * @return the lastname
     */
    public String getLastname() {
        return lastname;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the email
     */
    public String getEmail() {
        return email;
    }

    /**
     * @return the company
     */
    public String getCompany() {
        return company;
    }

    /**
     * @return the country
     */
    public String getCountry() {
        return country;
    }

    /**
     * @return the city
     */
    public String getCity() {
        return city;
    }

    /**
     * @return the region
     */
    public String getRegion() {
        return region;
    }

    /**
     * @return the timezone
     */
    public String getTimezone() {
        return timezone;
    }

    /**
     * @return the FiltertemplateList
     */
    public Map<String, String> getFiltertemplateList() {
        return FiltertemplateList;
    }

    /**
     * @return the AlertLevels
     */
    public AlertLevel getAlertLevels() {
        return AlertLevels;
    }

    /**
     * @return the FiltertemplateJson
     */
    public Map<String, Map<String, String>> getFiltertemplateMap() {
        return FiltertemplateMap;
    }

    /**
     * @return the TargetList
     */
    public Map<String, OddeeySenderMetricMetaList> getTargetList() {
        return TargetList;
    }

    /**
     * @return the balance
     */
    public Double getBalance() {
        return balance;
    }

    /**
     * @return the tmpconsumption
     */
    public Consumption getTmpconsumption() {
        return tmpconsumption;
    }

}
