package sniffer;

import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;

/**
 *
 * @author skuarch
 */
public class Connectivity implements Runnable {

    private static final Logger logger = Logger.getLogger(Connectivity.class);
    private ObjectMessage objectMessage = null;

    //==========================================================================
    public Connectivity(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    } // end connectivity

    //==========================================================================
    @Override
    public void run() {
        try {            
            new JMSProccessor().send("response connectivity", objectMessage, true);
        } catch (Exception e) {
            logger.error("run", e);
            new JMSProccessor().sendError(objectMessage, e.getMessage());
        }
    } // end run
} // end class
