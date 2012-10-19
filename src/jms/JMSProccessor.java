package jms;

import common.RequestDispatcher;
import java.io.Serializable;
import java.net.InetAddress;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TopicSubscriber;
import org.apache.log4j.Logger;
import util.PropertieWrapper;
import util.SerializableObject;

/**
 *
 * @author skuarch
 */
public class JMSProccessor extends JMS {

    private static final Logger logger = Logger.getLogger(JMSProccessor.class);
    private TopicSubscriber topicSubscriber = null;
    private ObjectMessage objectMessage = null;
    private MessageProducer messageProducer = null;
    private Message message = null;
    private String myName = null;

    //==========================================================================
    public JMSProccessor() {
        super();
        this.topicSubscriber = getTopicSubcriber();
        this.myName = PropertieWrapper.getMyName();
        objectMessage = getObjectMessage();
        messageProducer = getMessageProducer();
    } // end JMSProccessor

    //==========================================================================
    public synchronized void receive() {

        String tagTo = null;
        String sendTo = null;

        try {

            logger.info(" :: ready to receive messages :: ");

            while (true) {

                message = topicSubscriber.receive();

                if (message != null) {

                    tagTo = message.getStringProperty("tagTo");
                    sendTo = message.getStringProperty("sendTo");

                    //check if is for me
                    if (tagTo.equalsIgnoreCase("srs") && sendTo.equalsIgnoreCase(myName)) {

                        objectMessage = (ObjectMessage) message;
                        new Thread(new RequestDispatcher(objectMessage)).start();

                    }

                }

            } // end while

        } catch (Exception e) {
            logger.error("receive", e);
        } finally{
            shutdownConnection();
        }

    } // end receive

    //==========================================================================
    public synchronized void send(String string, String select, String sendTo, String tagTo, String type, String key, Object object) {

        if (object == null) {
            throw new NullPointerException("object is null");
        }

        String sendBy = null;
        String tagBy = null;

        try {

            sendBy = InetAddress.getLocalHost().getHostName();
            tagBy = "srs";

            logger.info("publish message to " + sendTo + " " + string);

            objectMessage.setStringProperty("select", select);
            objectMessage.setStringProperty("sendTo", sendTo);
            objectMessage.setStringProperty("sendBy", sendBy);
            objectMessage.setStringProperty("tagTo", tagTo);
            objectMessage.setStringProperty("tagBy", tagBy);
            objectMessage.setStringProperty("type", type);
            objectMessage.setStringProperty("key", key);
            objectMessage.setObject((Serializable) new SerializableObject().getSerializableObject(object));
            messageProducer.send(objectMessage);

        } catch (Exception e) {
            logger.error("send", e);
        } finally {
            shutdownConnection();
        }

    } // end send1

    //==========================================================================
    public synchronized void send(String string, Message message, Object object) {

        if (message == null) {
            throw new NullPointerException("message is null");
        }

        if (object == null) {
            throw new NullPointerException("object is null");
        }

        try {

            send(string, message.getStringProperty("select"), message.getStringProperty("sendBy"), message.getStringProperty("tagBy"), "response", message.getStringProperty("key"), object);

        } catch (Exception e) {
            logger.error("send2", e);
        }

    } // end send2

    //==========================================================================
    public synchronized void sendError(ObjectMessage objectMessage, String error) {

        if (objectMessage == null) {
            throw new NullPointerException("objectMessage is null");
        }

        if (error == null) {
            throw new NullPointerException("object is null");
        }


        try {

            objectMessage.setStringProperty("error", error);
            send("error", objectMessage.getStringProperty("select"), objectMessage.getStringProperty("sendBy"), objectMessage.getStringProperty("tagBy"), "response", objectMessage.getStringProperty("key"), error);

        } catch (Exception e) {
            logger.error("send2", e);
        }

    } // end sendError
} // end class

