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
public class NetworkProtocols implements Runnable {

    static final Logger logger = Logger.getLogger(NetworkProtocols.class);
    private ObjectMessage objectMessage = null;

    //=========================================================================

    public NetworkProtocols(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //=========================================================================

    @Override
    public void run() {
        try {
            networkProtocolsDataset();
        } catch (Exception e) {
            logger.error("ERROR NetworkProtocols().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================

    private void networkProtocolsDataset() {
        Object data = null;
        try {
            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")){
                data = createDatasetNetworkProtocolsTable(this.objectMessage);
            } else {
                data = createDatasetNetworkProtocols(this.objectMessage);
            }
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: NetworkProtocols().networkProtocolsDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread NetworkProtocols()");
        }
    }

    
    //=========================================================================

    static Object createDatasetNetworkProtocols(ObjectMessage objectMessage) {

        DefaultCategoryDataset dataset = null;
        ResultSet rs = null;
        int numProtocols = 0;
        String matrizProtocols[][] = null;

        try {
            dataset = new DefaultCategoryDataset();

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                rs = getDataSetNetworkProtocol(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetNetworkProtocol(objectMessage, false);
            }

            numProtocols = new DataBaseConnection().getNumRows(rs);
            matrizProtocols = new String[numProtocols][3];
            matrizProtocols = new MatrixProtocols().getMatrixProtocols(rs, "SELECT keyword FROM ethernet_protocols WHERE protocoldec = ", false);

            rs.close();
            rs = null;
            dataset = new DefaultCategoryDataset();

            for (int i = 0; i < matrizProtocols.length; i++) {
                String firstString = matrizProtocols[i][1];
                if (firstString != null) {
                    dataset.addValue(Double.parseDouble(matrizProtocols[i][1]), "netProt", matrizProtocols[i][2]);
                }
            }

            new DataBaseConnection().closeResultSet(rs);

        } catch (Exception e) {
            logger.error("ERROR NetworkProtocols().createDatasetNetworkProtocols(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR NetworkProtocols().createDatasetNetworkProtocols(): " + e);
        } finally {
            new DataBaseConnection().closeResultSet(rs);
            rs = null;
        }

        if (dataset == null) {
            logger.error("ERROR NetworkProtocols().createDatasetNetworkProtocols() dataset is null");
            new JMSProccessor().sendError(objectMessage, "createDatasetNetworkProtocols() dataset is null");
        }
        return (CategoryDataset) dataset;
    }

    //========================================================================

    static ResultSet getDataSetNetworkProtocol(ObjectMessage objectMessage, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String prefijo_convers = "";
        String prefijo_jobs = "";
        String limite = "";
        String limites[] = {"0", "0"};
        String ini = "";
        String addProtQuery = "";
        String select = "";
        String orderBy = "";

        try {
            prefijo_convers = new GetProperties("configuration/configuration.properties").getProperties("database.table.convers.prefijo");
            prefijo_jobs = new GetProperties("configuration/configuration.properties").getProperties("database.table.headers.prefijo");

            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];

            String extremos[] = Jobs.getJobExtremos(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                limites[1] = "25";
                limite = "limit " + (Integer.parseInt(limites[0]) - 1) + "," + limites[1];
            } else if (HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable") && countRows == false) {
                limite = " limit 0,25 ";
            } else {
                limite = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "type service").equalsIgnoreCase("not applicable")) {
                job_name = job_name.replace(prefijo_convers, prefijo_jobs);
                ini = "seg";
            } else {
                ini = "ini_conv";
            }


            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name,"",objectMessage);

            if(countRows){
                select = "SELECT COUNT(*) ";
                limite = "";
                orderBy = "";
            } else {
                select = "SELECT ether_type, sum(bytes) suma";
                orderBy = " order by suma desc ";
            }

            rs = new DataBaseConnection().executeQuery(select + " from " + job_name + " where ((" + ini + " >= " + extremos[0] + " and " + ini + "<= " + extremos[1] + ")" + addProtQuery + " ) group by ether_type " + orderBy + limite);
        } catch (Exception ex) {
            logger.error("ERROR: NetworkProtocols().getDataSetNetworkProtocol() " + ex);
            new JMSProccessor().sendError(objectMessage, "ERROR: NetworkProtocols().getDataSetNetworkProtocol()");
        }

        if (rs == null) {
            logger.error("ERROR NetworkProtocols().getDataSetNetworkProtocol rs is null");
            new JMSProccessor().sendError(objectMessage, "ERROR NetworkProtocols().getDataSetNetworkProtocol rs is null");
        }

        return rs;
    }

    //=========================================================================

    private Object createDatasetNetworkProtocolsTable(ObjectMessage objectMessage) {
        ArrayList al = null;
        ResultSet rs = null;
        String[] columnNames = null;
        Object[][] datas = null;
        double y = 0;
        double volGlobalP = 0;
        String[] limites = null;
        int filaInicial = 0;
        NumberFormat nf = null;



        try {

            al = new ArrayList();
            nf = NumberFormat.getInstance();

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                rs = getDataSetNetworkProtocol(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetNetworkProtocol(objectMessage, false);
            }

            int numProtocols = new DataBaseConnection().getNumRows(rs);
            String matrizProtocols[][] = new String[numProtocols][3];

            datas = new Object[new DataBaseConnection().getNumRows(rs) + 1][4];

            matrizProtocols = new MatrixProtocols().getMatrixProtocols(rs, "SELECT keyword FROM ethernet_protocols WHERE protocoldec = ", false);

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);


            columnNames = new String[4];
            columnNames[0] = "Num";
            columnNames[1] = "Protocol";
            columnNames[2] = "Bytes";
            columnNames[3] = "Percentage of use";

            if(!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")){
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            int i = 0;
            double sumTot = 0;

            for (i = 0; i < matrizProtocols.length; i++) {
                datas[i][0] = filaInicial;
                datas[i][1] = matrizProtocols[i][2];
                datas[i][2] = nf.format(Double.valueOf(matrizProtocols[i][1]));
                y = Double.valueOf((matrizProtocols[i][1]));
                datas[i][3] = (y * 100) / volGlobalP;

                sumTot += Double.valueOf(matrizProtocols[i][1]);
                filaInicial++;
            }

            datas[i][0] = filaInicial;
            datas[i][1] = "--- TOTAL ---";
            datas[i][2] = nf.format(sumTot);
            datas[i][3] = (sumTot * 100) / volGlobalP;


            new DataBaseConnection().closeResultSet(rs);
            al.add(columnNames);
            al.add(datas);

        } catch (Exception e) {
            logger.error("ERROR: NetworkProtocols().getDataSetNetworkProtocolsTable():  " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR NetworkProtocols().createDatasetNetworkProtocolsTable rs is null");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }
        return al;
    }
}
