/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.sql.ResultSet;
import javax.jms.ObjectMessage;
import org.apache.log4j.Logger;
import sniffer.AddProtocolQuery;
import sniffer.DataBaseConnection;
import sniffer.Jobs;

/**
 *
 * @author puebla
 */
public class VolumeUtilities {
    static final Logger logger = Logger.getLogger(VolumeUtilities.class);




    //========================================================================
    public double getVolumenGlobalPeriodo(ObjectMessage objectMessage) {
        String job_name = "";
        long volGlobalP = 0;
        ResultSet rs = null;
        AddProtocolQuery addPQ = null;

        try {
            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];
            addPQ = new AddProtocolQuery();

            String extremos[] = Jobs.getJobExtremos(objectMessage);

            rs = new DataBaseConnection().executeQuery("SELECT SUM(bytes) FROM " + job_name + " WHERE (ini_conv >= " + extremos[0] + " AND ini_conv <= " + extremos[1] + ")");
            rs.next();
            volGlobalP = rs.getLong(1);
            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR:  FillDataSet().getVolumenGlobalPeriodo():  " + e);
        }
        return volGlobalP;
    }
}
