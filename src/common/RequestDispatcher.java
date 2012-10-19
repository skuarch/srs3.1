package common;


import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import modeling.Chains;
import org.apache.log4j.Logger;
import sniffer.*;

/**
 *
 * @author skuarch
 */
public class RequestDispatcher extends Thread {

    private static final Logger logger = Logger.getLogger(RequestDispatcher.class);
    private ObjectMessage objectMessage = null;
    private String select = null;

    //==========================================================================
    public RequestDispatcher(ObjectMessage objectMessage) {        
        this.objectMessage = objectMessage;

    }

    //==========================================================================
    @Override
    public void run() {

  //HashMapUtilitie.getPropertiesNames(objectMessage);
        Thread t = null;
        try {
            logger.info("receiving message from " + objectMessage.getStringProperty("sendBy"));
            select = objectMessage.getStringProperty("select");
            
            //connectivity
            if (select.equalsIgnoreCase("connectivity")) {
               //new Thread(new Connectivity(objectMessage)).start();
                t = new Thread(new Connectivity(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else
            
            //jobs
            if(select.equalsIgnoreCase("getJobs")){
                t = new Thread(new Jobs(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //Bandwidth
            if(select.contains("Bandwidth")){
                t = new Thread(new Bandwidth(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else
            
            //Network Protocols
            if(select.contains("Network Protocols")){
                t = new Thread(new NetworkProtocols(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else
                
            //IP Protocols
            if(select.contains("IP Protocols")){
                t = new Thread(new IPProtocols(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //TCP Protocols
            if(select.contains("TCP Protocols")){
                t = new Thread(new TcpUdpProtocols(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //UDP Protocols
            if(select.contains("UDP Protocols")){
                t = new Thread(new TcpUdpProtocols(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //Conversations Bytes
            if(select.contains("Conversations Bytes")){
                String ipVersion = "_v4";
                t = new Thread(new ConversationsBytes(objectMessage,ipVersion));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //HOSTS
            if(select.contains("IP Talkers") || select.contains("Hostname Talkers")){
                String ipVersion = "_v4";
                t = new Thread(new HostTalkers(objectMessage,ipVersion));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //Web
            if(select.contains("Web Server Hosts")){
                String ipVersion = "_v4";
                t = new Thread(new Web(objectMessage, ipVersion));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //Type of Service
            if(select.contains("Type of Service")){
                t = new Thread(new TypeOfService(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //Top Ports
            if(select.contains("Ports")){
                t = new Thread(new TopPorts(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            } else

            //e2e
            if(select.contains("End to End")){
                t = new Thread(new E2E(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            }else
            
            //chains
            if(select.contains("chains")){
                t = new Thread(new Chains(objectMessage));
                t.setPriority(MAX_PRIORITY);
                t.start();
            }

        } catch (Exception e) {
            logger.error("run", e);
            new JMSProccessor().sendError(objectMessage, e.getMessage());
        } finally {
            t = null;
        }

    } // end run
} // end class
