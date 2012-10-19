package srs;

import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author skuarch
 */
public class Main {
	
    private static final Logger logger = Logger.getLogger(Main.class);

    //==========================================================================
    public Main() {
        PropertyConfigurator.configure("configuration/log.properties");
    } // Main

    //==========================================================================
    public static void main(String[] args) {

        new Main().logger.info("** program start **");        
        new JMSProccessor().receive();

    } // end main
    
} // end class

