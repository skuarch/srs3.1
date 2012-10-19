/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sniffer;

import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import util.GetProperties;
import util.HashMapUtilitie;
import util.VolumeUtilities;

import javax.jms.ObjectMessage;

/**
 *
 * @author puebla
 */
public class TopPorts implements Runnable {

    static final Logger logger = Logger.getLogger(TopPorts.class);
    private ObjectMessage objectMessage = null;
    private static final int TCP = 6;
    private static final int UDP = 17;

    //=========================================================================
    public TopPorts(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            topPortsDataset();
        } catch (Exception e) {
            logger.error("ERROR TopPorts().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================
    private void topPortsDataset() {
        Object data = null;
        try {

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")) {
                data = createDatasetTopPortsTable(this.objectMessage);
            } else {
                data = createDatasetTopPorts(this.objectMessage);
            }

            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: TopPorts().topPortsDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread topPortsDataset()");
        }
    }

    //=========================================================================
    private Object createDatasetTopPorts(ObjectMessage objectMessage) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;
        int protocol_type = 0;


        if (HashMapUtilitie.getProperties(objectMessage, "view").contains("TCP")) {
            protocol_type = TCP;
        } else if (HashMapUtilitie.getProperties(objectMessage, "view").contains("UDP")) {
            protocol_type = UDP;
        } else {
            protocol_type = 0;
        }

        try {

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                int numRows = 0;
                rs = getDataSetTopPorts(objectMessage, protocol_type, true);
                numRows = new DataBaseConnection().getNumRows(rs);
                rs = null;
                return numRows;
            } else {
                rs = getDataSetTopPorts(objectMessage, protocol_type, false);
            }

            dataset = new DefaultCategoryDataset();

            while (rs.next()) {
                dataset.addValue(rs.getDouble(4), " ", rs.getString(1));
            }

            rs.close();
            rs = null;
        } catch (Exception e) {
            logger.error("ERROR TopPorts().createDatasetTopPorts(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread TcpUdpProtocols()");
        }


        if (dataset == null) {
            logger.error("ERROR TopPorts().createDatasetTopPorts() dataset is null");
            new JMSProccessor().sendError(objectMessage, "TcpUdpProtocols().createDatasetTopPorts() dataset is null");
        }


        return (CategoryDataset) dataset;
    }

    //=========================================================================
    private Object createDatasetTopPortsTable(ObjectMessage objectMessage) {
        ArrayList al = null;
        ResultSet rs = null;
        NumberFormat format = null;
        String[] columnNames = null;
        Object[][] datas = null;
        double y = 0;
        double volGlobalP = 0;
        String[] limites = null;
        int filaInicial = 0;

        int protocol_type = 0;

        try {

            al = new ArrayList();

        if (HashMapUtilitie.getProperties(objectMessage, "view").contains("TCP")) {
            protocol_type = TCP;
        } else if (HashMapUtilitie.getProperties(objectMessage, "view").contains("UDP")) {
            protocol_type = UDP;
        } else {
            protocol_type = 0;
        }

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetTopPorts(objectMessage, protocol_type, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetTopPorts(objectMessage, protocol_type, false);
            }


            format = NumberFormat.getInstance();
            int numProtocols = new DataBaseConnection().getNumRows(rs);

            datas = new Object[numProtocols + 1][6];

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            columnNames = new String[6];
            columnNames[0] = "Num";
            columnNames[1] = "Port";
            columnNames[2] = "Bytes src";
            columnNames[3] = "Bytes dst";
            columnNames[4] = "Bytes tot";
            columnNames[5] = "Percentage of use";

            int i = 0;
            double sumTot = 0;
            double sumBytesSrc = 0;
            double sumBytesDst = 0;

            while (rs.next()) {
                datas[i][0] = filaInicial;
                datas[i][1] = rs.getString(1);
                datas[i][2] = format.format(rs.getDouble(2));
                datas[i][3] = format.format(rs.getDouble(3));
                datas[i][4] = format.format(rs.getDouble(4));
                y = rs.getDouble(2);
                datas[i][5] = (y * 100) / volGlobalP;
                sumBytesSrc += rs.getDouble(2);
                sumBytesDst += rs.getDouble(3);
                sumTot += rs.getDouble(4);
                filaInicial++;
                i++;
            }

            datas[i][0] = filaInicial;
            datas[i][1] = "--- TOTAL ---";
            datas[i][2] = format.format(sumBytesSrc);
            datas[i][3] = format.format(sumBytesDst);
            datas[i][4] = format.format(sumTot);
            datas[i][5] = (sumTot * 100) / volGlobalP;

            new DataBaseConnection().closeResultSet(rs);
            al.add(columnNames);
            al.add(datas);

        } catch (Exception e) {
            logger.error("ERROR: TcpUdpProtocols().createDatasetTcpUdpProtocolsTable():  " + e);
            new JMSProccessor().sendError(objectMessage, "Error creando DatasetTcpUdpProtocolsTable");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }
        return al;
    }

    //=========================================================================
    private ResultSet getDataSetTopPorts(ObjectMessage objectMessage, int protocol_type, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String limite = "";
        String limites[] = {"0", "0"};
        String orderBy = "";
        String prefijo_convers = "";
        String prefijo_jobs = "";
        String ini = "";

        String addProtQuery = "";
        String protType = "";


        try {

            prefijo_convers = new GetProperties("configuration/configuration.properties").getProperties("database.table.convers.prefijo");
            prefijo_jobs = new GetProperties("configuration/configuration.properties").getProperties("database.table.headers.prefijo");

            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];

            String extremos[] = Jobs.getJobExtremos(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                limites[1] = "25";
                limite = "limit " + (Integer.parseInt(limites[0]) - 1) + "," + limites[1];
                orderBy = "ORDER BY vol DESC";
            } else if (HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limite = " limit 0,25 ";
            } else {
                limite = "";
                orderBy = "";
            }

            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name, "", objectMessage);

            if (Boolean.parseBoolean(new GetProperties("configuration/configuration.properties").getProperties("database.use.headers"))) {
                job_name = job_name.replace(prefijo_convers, prefijo_jobs);
                ini = "seg";
            } else {
                ini = "ini_conv";
            }

            if (protocol_type == 0) {
                protType = "ip_protocol in (6,17)";
            } else {
                protType = "ip_protocol = " + protocol_type;
            }

            rs = new DataBaseConnection().executeQueryWhitCache("SELECT p, SUM(bytes_a), SUM(bytes_b), SUM(bytes_a + bytes_b) vol FROM ("
                    + "SELECT port_src p, SUM(bytes) bytes_a, 0 bytes_b "
                    + " FROM  " + job_name
                    + " WHERE ((" + protType + ")  AND (" + ini + " >= " + extremos[0] + " AND " + ini + " <= " + extremos[1] + ") AND port_src !=-11) " + addProtQuery
                    + " GROUP BY port_src "
                    + " UNION ALL "
                    + " SELECT port_dst p, 0 bytes_a, SUM(bytes) bytes_b"
                    + " FROM  " + job_name
                    + " WHERE ((" + protType + ")  AND (" + ini + " >= " + extremos[0] + " AND " + ini + " <= " + extremos[1] + ") AND port_dst !=-11) " + addProtQuery.replace("port_src_name", "port_dst_name")
                    + " GROUP BY port_dst "
                    + ")xx GROUP BY p "
                    + orderBy + " " + limite);
        } catch (Exception ex) {
            logger.error("ERROR: TopPorts().getDataSetTopPorts() " + ex);
            new JMSProccessor().sendError(objectMessage, "ERROR in query getDataSetTopPorts");
        }

        if (rs == null) {
            logger.error("ERROR TopPorts().getDataSetTopPorts rs is null");
            new JMSProccessor().sendError(objectMessage, "ERROR TopPorts().getDataSetTopPorts rs is null");
        }

        return rs;
    }
    //=========================================================================
}
