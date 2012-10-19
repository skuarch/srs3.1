/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sniffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.jfree.data.category.DefaultCategoryDataset;
import util.HashMapUtilitie;

/**
 *
 * @author puebla
 */
public class E2E implements Runnable {

    static final Logger logger = Logger.getLogger(E2E.class);
    private ObjectMessage objectMessage = null;

    //==========================================================================
    public E2E(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //==========================================================================
    @Override
    public void run() {

        try {
            E2EDataset();
        } catch (Exception e) {
            logger.error("ERROR IPProtocols().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //==========================================================================
    private void E2EDataset() {
        DefaultCategoryDataset dataset = null;
        String host = "";
        try {

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, 0);
            } else {
                if (HashMapUtilitie.getProperties(objectMessage, "e2e").equalsIgnoreCase("not applicable")) {
                    host = "localhost";
                } else {
                    host = HashMapUtilitie.getProperties(objectMessage, "e2e");
                }
                dataset = getDataSet(host);
                new JMSProccessor().send("response End to End", objectMessage, dataset);
            }
        } catch (Exception e) {
            logger.error("ERROR E2E().E2EDataset(): " + e);
        }
    }

    //==========================================================================
    private DefaultCategoryDataset getDataSet(String host) {
        Process process = null;
        InputStream is = null;
        BufferedReader br = null;
        String aux = null;
        String arrayHops[] = null;
        int i = 0;
        String hop_time[] = new String[2];
        DefaultCategoryDataset dataset = null;
        String seriesKey = "e2e";
        String comandoTraceroute = "";
        try {
            dataset = new DefaultCategoryDataset();
            comandoTraceroute = "/usr/bin/traceroute " + host + " -n -q 1";
            process = Runtime.getRuntime().exec(comandoTraceroute);
            is = process.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
            arrayHops = new String[30];
            aux = br.readLine();

            if (aux.startsWith("traceroute to ")) {
                aux = br.readLine();
                while (aux != null) {
                    aux = aux.substring(aux.indexOf("  ") + 2);
                    arrayHops[i++] = aux;
                    if (!aux.startsWith("*")) {
                        hop_time = aux.split("  ");
                        dataset.addValue(Double.parseDouble(hop_time[1].substring(0, hop_time[1].indexOf(" ms"))), seriesKey, hop_time[0]);
                    }

                    aux = br.readLine();
                }
            }
        } catch (Exception e) {
            logger.error("no se pudo ejecutar el comando: \"" + comandoTraceroute + "\"   error: "+ e);
        } finally {
            try {
                process.destroy();
                is.close();
                br.close();
            } catch (IOException ex) {
                logger.error("ERROR finalizando e2e: " + ex);
            }
        }
        return dataset;
    }
    //==========================================================================
}
