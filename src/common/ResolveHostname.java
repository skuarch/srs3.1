package common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author skuarch
 */
public class ResolveHostname {

    private final static Logger logger = Logger.getLogger(ResolveHostname.class);

    //==========================================================================
    public ResolveHostname() {
    } // end ResolveHostname    

    //==========================================================================
    public HashMap resolveHostname(HashMap hosts) {

        if (hosts == null || hosts.size() < 1) {
            logger.error("hosts is null or empty", new NullPointerException("hosts is null or empty"));
            return null;
        }

        Iterator iterator = null;
        Thread[] threadHosts = null;
        int i = 0;

        try {

            Map.Entry entry = null;
            iterator = hosts.entrySet().iterator();
            threadHosts = new Thread[hosts.size()];

            //run threads
            while (iterator.hasNext()) {

                entry = (Map.Entry) iterator.next();
                threadHosts[i] = new ThreadHosts(entry.getKey().toString(), hosts);
                threadHosts[i].start();

                i++;
            }
            

            for (int j = 0; j < threadHosts.length; j++) {
                threadHosts[j].join();
            }


        } catch (Exception e) {
            logger.error("resolveHostname", e);
        }

        return hosts;
    } // end resolveHostname
} // end run

