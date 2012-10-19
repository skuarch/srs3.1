package util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.jms.ObjectMessage;
import org.apache.log4j.Logger;

/**
 *
 * @author skuarch
 */
public class HashMapUtilitie {

    public static final Logger logger = Logger.getLogger(HashMapUtilitie.class);

    //==========================================================================
    public static HashMap getHashMap(ObjectMessage objectMessage) {

        if (objectMessage == null) {
            throw new NullPointerException("objectMesage is null");
        }

        HashMap hashMap = null;

        try {

            hashMap = (HashMap) objectMessage.getObject();

        } catch (Exception e) {
            logger.error("getHashMap", e);
        }

        return hashMap;

    } // end getHashMap

    //==========================================================================
    public static String getProperties(ObjectMessage objectMessage, String propertieName) {

        if (objectMessage == null) {
            throw new NullPointerException("objectMesage is null");
        }

        if (propertieName == null) {
            throw new NullPointerException("propertieName is null");
        }

        String value = null;
        HashMap hashmap = null;
        Iterator iterator = null;
        Map.Entry mapEntry = null;

        try {

            hashmap = getHashMap(objectMessage);

            if (hashmap == null) {
                return null;
            }

            iterator = hashmap.entrySet().iterator();

            while (iterator.hasNext()) {

                mapEntry = (Map.Entry) iterator.next();
                if (mapEntry.getKey().equals(propertieName)) {
                    value = mapEntry.getValue().toString();
                    break;
                }
            }

        } catch (Exception e) {
            logger.error("getProperties", e);
        } finally {
            hashmap = null;
            iterator = null;
            mapEntry = null;
            return value;
        }
        
    } // end getProperties

    

    //==========================================================================
    public static void getPropertiesNames(ObjectMessage objectMessage) {
        HashMap hashmap = null;
        Iterator iterator = null;
        Map.Entry mapEntry = null;

        try {
            hashmap = getHashMap(objectMessage);
            iterator = hashmap.entrySet().iterator();
            
            while (iterator.hasNext()) {
                mapEntry = (Map.Entry) iterator.next();
                System.out.println("key: " + mapEntry.getKey().toString() + "   ===>   "  + mapEntry.getValue().toString());

            }
        } catch (Exception e) {
            logger.error("getPropertiesNames ", e);
        }
    }//end getPropertiesNames

    
} // end class

