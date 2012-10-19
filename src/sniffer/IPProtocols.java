
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
public class IPProtocols implements Runnable {

    static final Logger logger = Logger.getLogger(TcpUdpProtocols.class);
    private ObjectMessage objectMessage = null;

    public IPProtocols(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    @Override
    public void run() {
        try {
            IpProtocolsDataset();
        } catch (Exception e) {
            logger.error("ERROR IPProtocols().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    private void IpProtocolsDataset() {
        Object data = null;
        try {

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")) {
                data = createDatasetIpProtocolsTable(this.objectMessage);
            } else {
                data = createDatasetIpProtocols(this.objectMessage);
            }
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: IPProtocols().IpProtocolsDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread IPProtocols()");
        }
    }

    private Object createDatasetIpProtocols(ObjectMessage objectMessage) {

        DefaultCategoryDataset dataset = null;
        ResultSet rs = null;
        int numProtocols = 0;
        String matrizProtocols[][] = null;

        try {
            dataset = new DefaultCategoryDataset();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                int numRows = 0;
                rs = getDataSetIPProtocol(objectMessage, true);
                numRows = new DataBaseConnection().getNumRows(rs);
                rs = null;
                return numRows;
            } else {
                rs = getDataSetIPProtocol(objectMessage, false);
            }

            numProtocols = new DataBaseConnection().getNumRows(rs);

            matrizProtocols = new String[numProtocols][3];

            matrizProtocols = new MatrixProtocols().getMatrixProtocols(rs, "SELECT keyword FROM ip_protocols WHERE pro_decimal =  ", false);

            dataset = new DefaultCategoryDataset();



            for (int i = 0; i < matrizProtocols.length; i++) {
                String firstString = matrizProtocols[i][1];
                if (firstString != null) {
                    dataset.addValue(Double.parseDouble(matrizProtocols[i][1]), "ipProt", matrizProtocols[i][2]); //busco en ethernet_protocol columna protocoldec y agruapamos las desconocidas y volvemos a ordenar, el nombre se toma de columna keyword
                }
            }
            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR createDatasetIPProtocols(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR createDatasetIPProtocols()");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }


        if (dataset == null) {
            logger.error("ERROR FillDataSet().createDatasetIPProtocols() dataset is null");
            new JMSProccessor().sendError(objectMessage, "ERROR createDatasetIPProtocols(): dataset is null");
        }

        return (CategoryDataset) dataset;
    }

    private ResultSet getDataSetIPProtocol(ObjectMessage objectMessage, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String limite = "";
        String limites[] = {"0", "0"};
        String ini = "";
        String prefijo_convers = "";
        String prefijo_jobs = "";
        String addProtQuery = "";
        String orderBy = "";
        String select = "";


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

            if (!HashMapUtilitie.getProperties(objectMessage, "type service").equalsIgnoreCase("not applicable")) {
                job_name = job_name.replace(prefijo_convers, prefijo_jobs);
                ini = "seg";
            } else {
                ini = "ini_conv";
            }

            if (countRows) {
                select = "SELECT COUNT(*) ";
                orderBy = "";
            } else {
                select = "SELECT ip_protocol, sum(bytes) suma";
                orderBy = " order by suma desc ";
            }
            rs = new DataBaseConnection().executeQuery(select + " from " + job_name + " where ((" + ini + " >= " + extremos[0] + " and " + ini + "<= " + extremos[1] + ") " + addProtQuery + " and ip_protocol >= 0 ) group by ip_protocol " + orderBy + limite);
        } catch (Exception ex) {
            logger.error("ERROR: FillDataSet().getDataSetIPProtocol() " + ex);
        }

        if (rs == null) {
            logger.error("ERROR FillDataSet().getDataSetIPProtocol rs is null");
            new JMSProccessor().sendError(objectMessage, "ERROR IPProtocols().getDataSetIPProtocol rs is null");
        }

        return rs;
    }

    private Object createDatasetIpProtocolsTable(ObjectMessage objectMessage) {
        ArrayList al = null;
        ResultSet rs = null;
        NumberFormat format = null;
        String[] columnNames = null;
        Object[][] datas = null;
        double y = 0;
        double volGlobalP = 0;
        String[] limites;
        int filaInicial = 0;
        AddProtocolQuery addPQ = null;

        try {
            addPQ = new AddProtocolQuery();
            al = new ArrayList();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetIPProtocol(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetIPProtocol(objectMessage, false);
            }

            format = NumberFormat.getInstance();

            int numProtocols = new DataBaseConnection().getNumRows(rs);

            String matrizProtocols[][] = new String[numProtocols][3];

            datas = new Object[new DataBaseConnection().getNumRows(rs) + 1][4];

            matrizProtocols = new MatrixProtocols().getMatrixProtocols(rs, "SELECT keyword FROM ip_protocols WHERE pro_decimal =  ", false);

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            columnNames = new String[4];
            columnNames[0] = "Num";
            columnNames[1] = "IP Protocol";
            columnNames[2] = "Bytes";
            columnNames[3] = "Percentage of use";

            int i = 0;
            double sumTot = 0;
            for (i = 0; i < matrizProtocols.length; i++) {
                datas[i][0] = filaInicial;
                datas[i][1] = matrizProtocols[i][2];
                datas[i][2] = format.format(Double.valueOf(matrizProtocols[i][1]));
                y = Double.valueOf((matrizProtocols[i][1]));
                datas[i][3] = (y * 100) / volGlobalP;

                sumTot += Double.valueOf(matrizProtocols[i][1]);
                filaInicial++;
            }

            datas[i][0] = filaInicial;
            datas[i][1] = "--- TOTAL ---";
            datas[i][2] = format.format(sumTot);
            datas[i][3] = (sumTot * 100) / volGlobalP;


            new DataBaseConnection().closeResultSet(rs);
            al.add(columnNames);
            al.add(datas);


        } catch (Exception e) {
            logger.error("ERROR FillDataSet().getDatasetIPProtocolsTable(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR IPProtocols().createDatasetIpProtocolsTable rs is null");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }

        return al;
    }
}
