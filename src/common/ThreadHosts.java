package common;

import java.net.InetAddress;
import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 *
 * @author skuarch
 */
public class ThreadHosts extends Thread {

    private static final Logger logger = Logger.getLogger(ThreadHosts.class);
    private String key = null;
    private HashMap hosts = null;

    //==========================================================================
    public ThreadHosts(String host, HashMap hosts) {
        this.key = host;
        this.hosts = hosts;
    } // end ThreadHosts

    //==========================================================================
    @Override
    public void run() {

        if (key == null || key.length() < 1) {
            logger.error("run", new NullPointerException("key is null or empty"));
            return;
        }

        if (hosts == null) {
            logger.error("run", new NullPointerException("hosts is null or empty"));
            return;
        }

        String hostname = key;       

        try {
            
            hostname = InetAddress.getByName(key).getHostName();            

        } catch (Exception e) {
            logger.error("run", e);
        } finally{
            hosts.put(key, hostname);
        }

    } // end run
} // end class
