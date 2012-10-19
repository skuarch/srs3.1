package util;

import javax.jms.MessageProducer;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 *
 * @author skuarch
 */
public class JMSUtilities {

    //=========================================================================
    public static void closeTopicSession(TopicSession topicSession) {

        try {

            if (topicSession != null) {
                topicSession.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            topicSession = null;
        }

    } // end closeTopic

    //=========================================================================
    public static void closeTopicConnection(TopicConnection topicConnection) {

        try {

            if (topicConnection != null) {
                topicConnection.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            topicConnection = null;
        }

    } // end closeTopicConnection

    //=========================================================================
    public static void closeTopicSubscriber(TopicSubscriber topicSubscriber) {

        try {

            if (topicSubscriber != null) {
                topicSubscriber.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            topicSubscriber = null;
        }

    } // end closeTopicSubscriber

    //==========================================================================
    public static void closeMessageProducer(MessageProducer messageProducer) {

        try {

            if (messageProducer != null) {
                messageProducer.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            messageProducer = null;
        }

    } // end closeMessageProducer
} // end class
