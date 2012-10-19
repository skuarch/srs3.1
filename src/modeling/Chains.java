package modeling;

import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import util.HashMapUtilitie;

/**
 *
 * @author skuarch
 */
public class Chains extends Thread {

    private static final Logger logger = Logger.getLogger(Chains.class);
    private ObjectMessage objectMessage = null;

    //==========================================================================
    public Chains(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //==========================================================================
    @Override
    public void run() {

        String view = null;

        try {

            view = HashMapUtilitie.getProperties(objectMessage, "view");

            if (view.equalsIgnoreCase("getChains")) {
                responseGetChains();
            } else 
                
            if (view.equalsIgnoreCase("deleteChain")) {
                System.out.println("delete chain"); 
                //delete chain without response
            } else
                
            if(view.equalsIgnoreCase("setEnabledChain")){
                // enabled or disabled chain withput reposponse
            }
            

        } catch (Exception e) {
            logger.error("Chains.run", e);
        }

    } // end run

    //==========================================================================
    private void responseGetChains() {

        Object[][] data = {
            {"AAA", "", "", ""}, {"CCC", "", "", ""}, {"BBB", "", "", ""}, {"mocos", "", "", ""}
        };

        try {

            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);

        } catch (Exception e) {
            logger.error("responseGetChains", e);
        }

    } // end responseGetChains
}
