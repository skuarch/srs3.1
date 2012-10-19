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
import util.HashMapUtilitie;
import util.VolumeUtilities;

/**
 *
 * @author puebla
 */
public class TypeOfService implements Runnable {

    static final Logger logger = Logger.getLogger(TypeOfService.class);
    private ObjectMessage objectMessage = null;

    //=========================================================================
    public TypeOfService(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            TypeOfServiceDataset();
        } catch (Exception e) {
            logger.error("ERROR Web().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    private void TypeOfServiceDataset() {
        Object data = null;
        try {
            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")) {
                data = createDatasetTypeOfServiceTable(this.objectMessage);
            } else {
                data = createDatasetTypeOfService(this.objectMessage);
            }
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: TypeOfService().TypeOfServiceDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread TypeOfServiceDataset()");
        }
    }

    //=========================================================================
    private Object createDatasetTypeOfService(ObjectMessage objectMessage) {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;

        String tosDSCP_class[] = null;
        try {

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetTypeOfServiceTraffic(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetTypeOfServiceTraffic(objectMessage, false);
            }


            tosDSCP_class = getTos_DscpClass(rs);
            dataset = new DefaultCategoryDataset();

            int i = 0;
            rs.beforeFirst();
            while (rs.next()) {
                dataset.addValue(rs.getDouble(2), "TypeOfService", tosDSCP_class[i]);
                i++;
            }
            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR FillDataSet().createDatasetWebSites(): " + e);
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }


        if (dataset == null) {
            logger.error("ERROR FillDataSet().createDatasetTypeOfService() dataset is null");
        }

        return (CategoryDataset) dataset;
    }

    //=========================================================================
    private String[] getTos_DscpClass(ResultSet rs) {
        String[] dscpClass = null;
        String dscp = "";
        ResultSet rs1 = null;
        String query = "";
        String tosDec = "";
        int filas = 0;


        try {
            filas = new DataBaseConnection().getNumRows(rs);

            dscpClass = new String[filas];

            int i = 0;
            while (rs.next()) {
                tosDec = rs.getString(1);

                query = "SELECT dscp_class FROM type_of_service WHERE tos_dec = " + tosDec;

                rs1 = new DataBaseConnection().executeQuery(query);
                if (rs1.next()) {
                    dscp = rs1.getString(1);
                } else {
                    dscp = "Unknown-" + tosDec;
                }
                dscpClass[i] = dscp;
                dscp = null;
                rs1.close();
                i++;
            }
        } catch (Exception e) {
            logger.error("ERROR TypeOfService().getTos_DscpClass():  " + e);
        }

        return dscpClass;
    }

    //=========================================================================
    private ResultSet getDataSetTypeOfServiceTraffic(ObjectMessage objectMessage, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String extremos[] = {"0", "0"};
        String limite = "";
        String limites[] = {"0", "0"};
        String addProtQuery = "";
        String select = "";
        String orderBy = "";

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

            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name, "", objectMessage);

            if (countRows) {
                select = "SELECT tos ";
                orderBy = "";
            } else {
                select = "SELECT tos, SUM(bytes) AS vol ";
                orderBy = " ORDER BY vol DESC ";
            }
            rs = new DataBaseConnection().executeQuery(select + " FROM " + job_name
                    + " WHERE (ini_conv >= " + extremos[0] + " AND fin_conv <= " + extremos[1] + ") AND (tos != -1) "
                    + addProtQuery
                    + " GROUP BY tos " + orderBy + limite);

        } catch (Exception ex) {
            logger.error("ERROR: TypeOfService().getDataSetTypeOfServiceTraffic() " + ex);
        }
        if (rs == null) {
            logger.error("ERROR TypeOfService().getDataSetTypeOfServiceTraffic rs is null");
            new JMSProccessor().sendError(objectMessage, "ToS ResultSet is null");
        }
        return rs;
    }

    private Object createDatasetTypeOfServiceTable(ObjectMessage objectMessage) {
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
        String[] tosDSCP_class = null;

        try {
            al = new ArrayList<Object>();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                rs = getDataSetTypeOfServiceTraffic(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetTypeOfServiceTraffic(objectMessage, false);
            }

            tosDSCP_class = getTos_DscpClass(rs);
            datas = new Object[new DataBaseConnection().getNumRows(rs) + 1][4];

            nf = NumberFormat.getInstance();

            columnNames = new String[4];
            columnNames[0] = "Num";
            columnNames[1] = "Type Of Service";
            columnNames[2] = "Bytes";
            columnNames[3] = "Percentage of use";

            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            double sumTot = 0;

            while (rs.next()) {
                datas[i][0] = filaInicial;
                datas[i][1] = tosDSCP_class[i];
                datas[i][2] = nf.format(Double.valueOf(rs.getString(2)));
                y = Double.valueOf(rs.getString(2));
                datas[i][3] = (y * 100) / volGlobalP;

                sumTot += Double.valueOf(rs.getString(2));
                i++;
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
            logger.error("ERROR: TypeOfService().createDatasetTypeOfServiceTable():  " + e);
            new JMSProccessor().sendError(objectMessage, "Al obtener el resultSet de ToS ");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }
        return al;
    }
    //=========================================================================
}
