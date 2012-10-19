package sniffer;

import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import util.HashMapUtilitie;
import util.Subnet;
import util.VolumeUtilities;

/**
 *
 * @author puebla
 */
public class Web implements Runnable {

    static final Logger logger = Logger.getLogger(Web.class);
    private ObjectMessage objectMessage = null;
    String ipVersion = null;

    //=========================================================================
    public Web(ObjectMessage objectMessage, String ipVersion) {
        this.objectMessage = objectMessage;
        this.ipVersion = ipVersion;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            WebProtocolsDataset();
        } catch (Exception e) {
            logger.error("ERROR Web().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================
    private void WebProtocolsDataset() {
        Object data = null;
        try {
            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")) {
                data = createDatasetWebProtocolsTable(this.objectMessage);
            } else {
                data = createDatasetWebProtocols(this.objectMessage);
            }
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: Web().WebProtocolsDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread WebProtocolsDataset()");
        }
    }

    //=========================================================================
    private Object createDatasetWebProtocols(ObjectMessage objectMessage) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;


        try {
            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetWebSitesTraffic(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetWebSitesTraffic(objectMessage, false);
            }

            dataset = new DefaultCategoryDataset();

            while (rs.next()) {
                dataset.addValue(rs.getDouble(3), "Websites", rs.getString(2));
            }
            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR Web().createDatasetWebProtocols(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread createDatasetWebProtocols()");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }


        if (dataset == null) {
            logger.error("ERROR Web().createDatasetWebProtocols() dataset is null");
            new JMSProccessor().sendError(objectMessage, "ERROR in thread createDatasetWebProtocols() dataset is null");
        }

        return (CategoryDataset) dataset;
    }

    //=========================================================================
    private Object createDatasetWebProtocolsTable(ObjectMessage objectMessage) {
        ArrayList<Object> al = null;
        ResultSet rs = null;
        String[] columnNames = null;
        Object[][] datas = null;
        int i = 0;
        double y = 0;
        double volGlobalP = 0;
        String[] limites;
        int filaInicial = 1;
        NumberFormat nf = null;


        try {
            al = new ArrayList<Object>();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetWebSitesTraffic(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetWebSitesTraffic(objectMessage, false);
            }


            datas = new Object[new DataBaseConnection().getNumRows(rs) + 1][5];

            nf = NumberFormat.getInstance();

            columnNames = new String[5];
            columnNames[0] = "Num";
            columnNames[1] = "IP site web";
            columnNames[2] = "Web Server Host";
            columnNames[3] = "Bytes";
            columnNames[4] = "Percentage of use";

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            double sumTot = 0;

            while (rs.next()) {
                datas[i][0] = filaInicial;
                datas[i][1] = new Subnet().inet_ntoa(rs.getString(1));
                datas[i][2] = rs.getString(2);
                datas[i][3] = nf.format(Double.valueOf(rs.getString(3)));
                y = Double.valueOf(rs.getString(3));
                datas[i][4] = (y * 100) / volGlobalP;

                sumTot += Double.valueOf(rs.getString(3));
                i++;
                filaInicial++;
            }



            datas[i][0] = filaInicial;
            datas[i][1] = "--- TOTAL ---";
            datas[i][2] = "";
            datas[i][3] = nf.format(sumTot);
            datas[i][4] = (sumTot * 100) / volGlobalP;


            new DataBaseConnection().closeResultSet(rs);
            al.add(columnNames);
            al.add(datas);

        } catch (Exception e) {
            logger.error("ERROR: Web().createDatasetWebProtocolsTable():  " + e);
            new JMSProccessor().sendError(objectMessage, "createDatasetWebProtocolsTable error");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }
        return al;
    }

    //=========================================================================
    private ResultSet getDataSetWebSitesTraffic(ObjectMessage objectMessage, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String extremos[] = null;
        String limite = "";
        String limites[] = {"0", "0"};
        String ini = "";
        String addProtQuery = "";
        String orderBy = "";
//        String referer_host = "";


        try {

            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];
            extremos = Jobs.getJobExtremos(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                limites[1] = "25";
                limite = "limit " + (Integer.parseInt(limites[0]) - 1) + "," + limites[1];
            } else if (HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limite = " limit 0,25 ";
            } else {
                limite = "";
            }

            if (countRows) {
                orderBy = "";
            } else {
                orderBy = " ORDER BY vol DESC ";
            }

            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name, "", objectMessage);
            ini = "ini_conv";

            rs = new DataBaseConnection().executeQuery("SELECT INET_NTOA(IF(port_src < port_dst, ip_src" + ipVersion + ", ip_dst" + ipVersion + ")) ip, host_web, SUM(bytes)  AS vol FROM "
                    + job_name
                    + " WHERE (host_web != \"null\"  AND host_web != \"\") AND (" + ini + " >= " + extremos[0] + " AND " + ini + " <= " + extremos[1] + ") "
                    + addProtQuery
                    + " GROUP BY host_web " + orderBy + limite);

        } catch (Exception ex) {
            logger.error("ERROR: Web().getDataSetWebSitesTraffic() " + ex);
            new JMSProccessor().sendError(objectMessage, "getDataSetWebSitesTraffic error");
        }
        if (rs == null) {
            logger.error("ERROR Web().getDataSetWebSitesTraffic rs is null");
            new JMSProccessor().sendError(objectMessage, "ERROR getDataSetWebSitesTraffic resultSet is null");
        }
        return rs;
    }
    //=========================================================================
}
