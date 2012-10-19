/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sniffer;

import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import util.GetProperties;
import util.HashMapUtilitie;
import util.VolumeUtilities;

/**
 *
 * @author puebla
 */
public class TcpUdpProtocols implements Runnable {
    static final Logger logger = Logger.getLogger(TcpUdpProtocols.class);
    private ObjectMessage objectMessage = null;
    private static final int TCP = 6;
    private static final int UDP = 17;

    //=========================================================================
    public TcpUdpProtocols(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            tcpUdpProtocolsDataset();
        } catch (Exception e) {
            logger.error("ERROR TcpUdpProtocols().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================

    private void tcpUdpProtocolsDataset() {
        Object data = null;
        try {

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")){
                data = createDatasetTcpUdpProtocolsTable(this.objectMessage);
            } else {
                data = createDatasetTcpUdpProtocols(this.objectMessage);
            }

            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: TcpUdpProtocols().tcpUdpProtocolsDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread TcpUdpProtocolsDataSet()");
        }
    }

    //=========================================================================

    private Object createDatasetTcpUdpProtocols(ObjectMessage objectMessage) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;
        int protocol_type = 0;


        if(HashMapUtilitie.getProperties(objectMessage, "view").contains("TCP")){
           protocol_type = TCP;
        } else {
            protocol_type = UDP;
        }

        try {

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                int numRows = 0;
                rs = getDataSetTCPoUDPProtocol(objectMessage, protocol_type, true);
                numRows = new DataBaseConnection().getNumRows(rs);
                rs = null;
                return numRows;
            } else {
                 rs = getDataSetTCPoUDPProtocol(objectMessage, protocol_type, false);
            }

            dataset = new DefaultCategoryDataset();

            while(rs.next()){
                dataset.addValue(rs.getDouble(2), " " , rs.getString(1));                
            }

            rs.close();
            rs = null;
        } catch (Exception e) {
            logger.error("ERROR TcpUdpProtocols().createDatasetTCPoUDPProtocols(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread TcpUdpProtocols()");
        }


        if (dataset == null) {
            logger.error("ERROR TcpUdpProtocols().createDatasetTCPoUDPProtocols() dataset is null");
            new JMSProccessor().sendError(objectMessage, "TcpUdpProtocols().createDatasetTCPoUDPProtocols() dataset is null");
        }


        return (CategoryDataset) dataset;
    }

    //=========================================================================

    private ResultSet getDataSetTCPoUDPProtocol(ObjectMessage objectMessage, int protocol_type, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";        
        String limite = "";
        String limites[] = {"0", "0"};
        String orderBy = "";
        String prefijo_convers = "";
        String prefijo_jobs = "";
        //String select = "";
        String ini = "";

        String addProtQuery = "";


        try {

            prefijo_convers = new GetProperties("configuration/configuration.properties").getProperties("database.table.convers.prefijo");
            prefijo_jobs = new GetProperties("configuration/configuration.properties").getProperties("database.table.headers.prefijo");

            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];

            String extremos[] = Jobs.getJobExtremos(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                limites[1] = "25";
                limite = "limit " + (Integer.parseInt(limites[0]) - 1) + "," + limites[1];
                orderBy = "ORDER BY vt DESC";
            } else if (HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limite = " limit 0,25 ";
            } else {
                limite = "";
                orderBy = "";
            }



            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name,"",objectMessage);

            if (Boolean.parseBoolean(new GetProperties("configuration/configuration.properties").getProperties("database.use.headers"))) {
                job_name = job_name.replace(prefijo_convers, prefijo_jobs);
                ini = "seg";
            } else {
                ini = "ini_conv";
            }

            rs = new DataBaseConnection().executeQueryWhitCache("SELECT port_name, SUM(bytes) vt FROM " + job_name + " WHERE ((ip_protocol = " + protocol_type + ") AND (" + ini + " >= " + extremos[0] + " AND " + ini + " <= " + extremos[1] + ") " + addProtQuery + " AND port_name!= '-11') GROUP BY port_name " + orderBy + " " + limite);
        } catch (Exception ex) {
            logger.error("ERROR: TcpUdpProtocols().getDataSetTCPoUDPProtocol() " + ex);
            new JMSProccessor().sendError(objectMessage,"ERROR in query getDataSetTCPoUDPProtocol");
        }

        if (rs == null) {
            logger.error("ERROR TcpUdpProtocols().getDataSetTCPoUDPProtocol rs is null");
            new JMSProccessor().sendError(objectMessage,"ERROR TcpUdpProtocols().getDataSetTCPoUDPProtocol rs is null");
        }

        return rs;
    }

    //=========================================================================

    private Object createDatasetTcpUdpProtocolsTable(ObjectMessage objectMessage) {
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

        if(HashMapUtilitie.getProperties(objectMessage, "view").contains("TCP")){
           protocol_type = TCP;
        } else {
            protocol_type = UDP;
        }

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                rs = getDataSetTCPoUDPProtocol(objectMessage, protocol_type, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetTCPoUDPProtocol(objectMessage, protocol_type, false);
            }


            format = NumberFormat.getInstance();
            int numProtocols = new DataBaseConnection().getNumRows(rs);

            datas = new Object[numProtocols + 1][4];

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if(!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")){
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            columnNames = new String[4];
            columnNames[0] = "Num";
            columnNames[1] = "Protocol";
            columnNames[2] = "Bytes";
            columnNames[3] = "Percentage of use";

            int i = 0;
            double sumTot = 0;
            while(rs.next()) {
                datas[i][0] = filaInicial;
                datas[i][1] = rs.getString(1);
                datas[i][2] = format.format(rs.getDouble(2));
                y = rs.getDouble(2);
                datas[i][3] = (y * 100) / volGlobalP;

                sumTot += rs.getDouble(2);
                filaInicial++;
                i++;
            }

            datas[i][0] = filaInicial;
            datas[i][1] = "--- TOTAL ---";
            datas[i][2] = format.format(sumTot);
            datas[i][3] = (sumTot * 100) / volGlobalP;

            new DataBaseConnection().closeResultSet(rs);
            al.add(columnNames);
            al.add(datas);

        } catch (Exception e) {
            logger.error("ERROR: TcpUdpProtocols().createDatasetTcpUdpProtocolsTable():  " + e);
            new JMSProccessor().sendError(objectMessage,"Error creando DatasetTcpUdpProtocolsTable");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }
        return al;
    }
}
