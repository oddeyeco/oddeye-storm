/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.OddeyeHttpURLConnection;
import co.oddeye.core.globalFunctions;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SendToTelegram extends SendTo {

    public static final Logger LOGGER = LoggerFactory.getLogger(SendToTelegram.class);
    private final OddeeySenderMetricMetaList targetdata;
    private final Map.Entry<String, StormUser> targetuser;    

    public SendToTelegram(OddeeySenderMetricMetaList value, Map.Entry<String, StormUser> user) {
        targetdata = value;
        targetuser = user;
    }

    @Override
    public void run() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Sent for user " + targetuser.getValue().getEmail() + " to Telegram " + targetdata.size() + " Messages");
        }

        Iterator<Map.Entry<String, OddeeyMetricMeta>> iter = targetdata.entrySet().iterator();
        String Text = "";
        int Counter = 0;
        while (iter.hasNext()) {
            Map.Entry<String, OddeeyMetricMeta> entry = iter.next();
            if (targetdata.getLastSendList().containsKey(entry.getValue().sha256Code())) {
                if (entry.getValue().getErrorState().getLevel() == targetdata.getLastSendList().get(entry.getValue().sha256Code())) {
                    if (LOGGER.isInfoEnabled())
                    {
                        LOGGER.info("Not send returned level");
                    }                    
                    iter.remove();
                    continue;
                }
            }                        
            Counter++;            
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Sent metric " + entry.getValue().sha256Code()+" Name:"+entry.getValue().getName() + " State " + entry.getValue().getErrorState().getStateName() + " to Level " + entry.getValue().getErrorState().getLevelName() + "Tags:  " + entry.getValue().getTags());
            }            
            if (Counter < 11) {
//                if (entry.getValue().getErrorState().getLevel() == -1) {
//                    Text = Text + "\nMertic " + "<a href=\"" + "https://app.oddeye.co/OddeyeCoconut/metriq/" + entry.getValue().sha256Code() + "/" + (long) Math.floor(entry.getValue().getErrorState().getTime() / 1000) + "\">" + entry.getValue().getName() + "</a> <b> Already not Error </b> <code>\nTags:\n " + entry.getValue().getDisplayTags("\n ") + "</code>\n";
//                } else {
//                    Text = Text + "\nLevel For " + "<a href=\"" + "https://app.oddeye.co/OddeyeCoconut/metriq/" + entry.getValue().sha256Code() + "/" + (long) Math.floor(entry.getValue().getErrorState().getTime() / 1000) + "/\">" + entry.getValue().getName() + "</a> <b>" + entry.getValue().getErrorState().getStateName() + " to " + entry.getValue().getErrorState().getLevelName() + "</b> <code> \nTags:\n " + entry.getValue().getDisplayTags("\n ") + "</code>";
//                }

                if (entry.getValue().getErrorState().getLevel() == -1) {
                    Text = Text + "\n<a href=\"" + "https://app.oddeye.co/OddeyeCoconut/metriq/" + entry.getValue().sha256Code() + "/" + (long) Math.floor(entry.getValue().getErrorState().getTime() / 1000) + "\">" + entry.getValue().getName() + "</a> <b> : OK </b> <code>\nTags:\n " + entry.getValue().getDisplayTags("\n ",targetdata.getTargetOption()) + "</code>\n";
                } else {//+ entry.getValue().getErrorState().getStateName()
                    Text = Text + "\n<a href=\"" + "https://app.oddeye.co/OddeyeCoconut/metriq/" + entry.getValue().sha256Code() + "/" + (long) Math.floor(entry.getValue().getErrorState().getTime() / 1000) + "/\">" + entry.getValue().getName() + "</a> <b> :" + entry.getValue().getErrorState().getStateChar()+" "+entry.getValue().getErrorState().getLevelName() + "</b> <code> \nTags:\n " + entry.getValue().getDisplayTags("\n ",targetdata.getTargetOption()) + "</code>";
                }
                
                
                targetdata.getLastSendList().put(entry.getValue().sha256Code(), entry.getValue().getErrorState().getLevel());
                iter.remove();
            }
            else
            {
                targetdata.clear();
                break;
            }

        }
        
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Send Telegram text "+ Text);
            }          
//        LOGGER.warn(Text);
        if ((!targetdata.getTargetValue().isEmpty()) && (!Text.isEmpty())) {
            if (Counter > 10) {
                Text = Text + "\n And " + (Counter - 10) + " More";
            }
            try {
                Text = "<i>OddEye Report</i>" + Text;
                Text = URLEncoder.encode(Text, "UTF-8");
                String uri = "https://api.telegram.org/bot317219245:AAFqFjcddeXfrpIZ-V4ENeve87oxg0ZGGYs/sendMessage?chat_id=" + targetdata.getTargetValue() + "&text=" + Text + "&parse_mode=HTML&disable_web_page_preview=true";
                OddeyeHttpURLConnection.sendGet(uri);
            } catch (UnsupportedEncodingException ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            } catch (Exception ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            }
        }

    }

}
