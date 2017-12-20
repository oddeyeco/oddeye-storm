/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.storm.core;

import co.oddeye.core.OddeeyMetricMeta;
import co.oddeye.core.OddeeySenderMetricMetaList;
import co.oddeye.core.OddeyeMessage;
import co.oddeye.core.globalFunctions;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import javax.mail.Authenticator;
import javax.mail.Session;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;

import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author vahan
 */
public class SendToEmail extends SendTo {

    public static final Logger LOGGER = LoggerFactory.getLogger(SendToEmail.class);
    private OddeeySenderMetricMetaList targetdata;
    private final Map.Entry<String, StormUser> targetuser;
    private final Session session;
    private final String from;

    public SendToEmail(OddeeySenderMetricMetaList value, Map.Entry<String, StormUser> user, Map conf) {

        try {
            targetdata = value.clone();
        } catch (CloneNotSupportedException ex) {
            targetdata = new OddeeySenderMetricMetaList();
            LOGGER.error(globalFunctions.stackTrace(ex));
        }

        targetuser = user;

        // Sender's email ID needs to be mentioned
        from = String.valueOf(conf.get("from"));

        // Get system properties
        Properties properties = new Properties();

        properties.put("mail.smtp.auth", String.valueOf(conf.get("smtp.auth")));
        properties.put("mail.smtp.port", String.valueOf(conf.get("smtp.port")));
        properties.put("mail.smtp.host", String.valueOf(conf.get("smtp.host")));
        properties.put("mail.user", String.valueOf(conf.get("mail.user")));
        properties.put("mail.password", String.valueOf(conf.get("mail.password")));

        // Get the default Session object.
        String username = String.valueOf(conf.get("mail.user"));
        String password = String.valueOf(conf.get("mail.password"));
        session = Session.getInstance(properties, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(username, password);
            }
        });
    }

    @Override
    public void run() {
//        LOGGER.warn("Sent for user " + targetuser.getValue().getEmail() + " to Email " + targetdata.size() + " Messages");
        Iterator<Map.Entry<Integer, OddeeyMetricMeta>> iter = targetdata.entrySet().iterator();
        String Text = "";
        String HTML = "";

        while (iter.hasNext()) {

            Map.Entry<Integer, OddeeyMetricMeta> entry = iter.next();

            if (entry.getValue().getErrorState().getLevel() == -1) {
                HTML = "<div>" + HTML + "<br>Mertic:" + entry.getValue().getName() + "<br>Tags:<br>" + entry.getValue().getDisplayTags("<br>") + " Already not in Error " + "</div>";
                Text = "/n" + Text + "/nMertic:" + entry.getValue().getName() + "/nTags:/n" + entry.getValue().getDisplayTags("/n") + " Already not in Error " + "/n";
            } else {
                HTML = "<div>" + HTML + "<br>Level For Metric:" + entry.getValue().getName() + "<br>Tags:<br>" + entry.getValue().getDisplayTags("<br>") + " " + entry.getValue().getErrorState().getStateName() + " to " + entry.getValue().getErrorState().getLevelName() + "</div>";
                Text = "/n" + Text + "/nLevel For Metric:" + entry.getValue().getName() + "/nTags:/n" + entry.getValue().getDisplayTags("/n") + " " + entry.getValue().getErrorState().getStateName() + " to " + entry.getValue().getErrorState().getLevelName() + "/n";
            }

            iter.remove();
        }
//        LOGGER.warn(Text);
        if (!HTML.isEmpty()) {
            // Recipient's email ID needs to be mentioned.
            String to = targetdata.getTargetValue();
//            LOGGER.warn("targetdata.getTargetValue(); " + targetdata.getTargetValue());
            try {
                MimeMessage message = new OddeyeMessage(session);
                message.addHeader("Content-type", "text/HTML; charset=UTF-8");
                message.addHeader("format", "flowed");
                message.addHeader("Content-Transfer-Encoding", "8bit");

                String pattern = "EEE, dd MMM yyyy HH:mm:ss Z";
                SimpleDateFormat format = new SimpleDateFormat(pattern);
                message.addHeader("Date", format.format(Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime()));

                // Set From: header field of the header.
                message.setFrom(new InternetAddress(from, "OddEye Analytic"));
                message.setReplyTo(InternetAddress.parse(from, false));

                // Set To: header field of the header.
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

                // Set Subject: header field
                message.setSubject("OddEye Report");

                final MimeBodyPart textPart = new MimeBodyPart();
                textPart.setContent(Text, "text/plain");
                // HTML version
                final MimeBodyPart htmlPart = new MimeBodyPart();
                htmlPart.setContent("<html lang=\"en\"><body>" + HTML + "</body></html>", "text/html");

                final Multipart mp = new MimeMultipart("alternative");
                mp.addBodyPart(textPart);
                mp.addBodyPart(htmlPart);
                // Send the actual HTML message, as big as you like
                message.setContent(mp);

                // Send the actual HTML message, as big as you like
//                message.setContent(HTML, "text/html");
//                message.setHeader("Message-ID", Math.random()+"@oddeye.co");
// Send message
                Transport.send(message);
//                LOGGER.warn("mail sended");
            } catch (MessagingException | UnsupportedEncodingException ex) {
                LOGGER.error(globalFunctions.stackTrace(ex));
            }
        } else {
        }

    }

}
