
package sniffer;

import common.ResolveHostname;
import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
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
public class HostTalkers implements Runnable {

    static final Logger logger = Logger.getLogger(HostTalkers.class);
    private ObjectMessage objectMessage = null;
    String ipVersion = null;

    //=========================================================================

    public HostTalkers(ObjectMessage objectMessage, String ipVersion) {
        this.objectMessage = objectMessage;
        this.ipVersion = ipVersion;
    }

    //=========================================================================

    @Override
    public void run() {
        try {
            HostTalkersDataset();
        } catch (Exception e) {
            logger.error("ERROR HostTalkers().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get HostTalkers dataset");
        }
    }

    //=========================================================================

    private void HostTalkersDataset() {
        Object data = null;
        try {
            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Table")){
                data = createDatasetHostTalkersTable(this.objectMessage);
            } else {
                data = createDatasetHostTalkers(this.objectMessage);
            }
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: HostTalkersDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread HostTalkersDataset()");
        } finally{
            data = null;
        }
    }

    //=========================================================================

    private Object createDatasetHostTalkers(ObjectMessage objectMessage) {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;

        try {
            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                rs = getDataSetHostTalkers(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetHostTalkers(objectMessage, false);
            }


            String solicitado = HashMapUtilitie.getProperties(objectMessage, "view");

            

            dataset = new DefaultCategoryDataset();

            String ip = null;
            HashMap hostnames = new HashMap();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Hostname Talkers")) {
                while (rs.next()) {
                    ip = new Subnet().inet_ntoa(rs.getString(1));                    
                    hostnames.put(ip, "");
                }
                hostnames = new ResolveHostname().resolveHostname(hostnames);
            }

            rs.beforeFirst();

            while (rs.next()) {
                ip = new Subnet().inet_ntoa(rs.getString(1));
                if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Hostname Talkers")) {
                    ip = (String) hostnames.get(ip);
                }
                dataset.addValue(Double.parseDouble(rs.getString(2)), solicitado , ip);
            }

            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR HostTalkers().createDatasetHostTalkers(): " + e);
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }


        if (dataset == null) {
            logger.error("ERROR HostTalkers().createDatasetHostTalkers() dataset is null");
        }
        return (CategoryDataset) dataset;
    }

    //=========================================================================

    private ResultSet getDataSetHostTalkers(ObjectMessage objectMessage, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String extremos[] = null;
        String limite = "";
        String limites[] = {"0", "0"};
        String orderBy = "";
        String ini = "";
        String addProtQuery = "";
        String query = "";
        String select = "";
        String Talker_Or_Src_Or_Dst = "";

        try {
            Talker_Or_Src_Or_Dst = HashMapUtilitie.getProperties(objectMessage, "view");

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
                orderBy = "";
            }

            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name,"",objectMessage);

            if (Talker_Or_Src_Or_Dst.contains("Talkers Sources Bytes") && !countRows) {
                select = "SELECT (ip_src" + ipVersion +") as ip, sum(bytes) AS vto, SUM(paquetes)packets";
                orderBy = "ORDER BY vto DESC";
            } else if (Talker_Or_Src_Or_Dst.contains("Talkers Destinations Bytes") && !countRows) {
                select = "SELECT (ip_dst" + ipVersion +") AS ip, sum(bytes) AS vto, SUM(paquetes)packets ";
                orderBy = "ORDER BY vto DESC";
            } else if (Talker_Or_Src_Or_Dst.contains("Talkers Bytes") && !countRows) {
                select = "select (ip)ipa, sum(vt)vto, SUM(packets) ";
                orderBy = "ORDER BY vto DESC";
            }

            ini = "ini_conv";

            if (countRows) {
                if(Talker_Or_Src_Or_Dst.contains("Talkers Sources")){
                    select = "select (ip_src" + ipVersion +")ip ";
                } else if (Talker_Or_Src_Or_Dst.contains("Talkers Destinations Bytes")){
                    select = "select (ip_dst" + ipVersion +")ip ";
                } else {
                    select = "select (ip)ipa ";
                }
                orderBy = "";
            }


            if (Talker_Or_Src_Or_Dst.contains("Talkers Bytes")){
                query = select + " from ("
                        + " SELECT (ip_src" + ipVersion +") as ip, sum(bytes) AS vt, SUM(paquetes)packets  FROM " + job_name + " WHERE (" + ini + " >= " + extremos[0] + " AND " + ini + "<= " + extremos[1] + ") AND (ip_src" + ipVersion +" > 0) " + addProtQuery + " GROUP BY ip "
                        + " union all "
                        + " SELECT (ip_dst" + ipVersion +") AS ip, sum(bytes) AS vt, SUM(paquetes)packets  FROM " + job_name + " WHERE (" + ini + " >= " + extremos[0] + " AND " + ini + "<= " + extremos[1] + ") AND (ip_dst" + ipVersion +" > 0) " + addProtQuery.replace("OR ip_dst" + ipVersion, "OR ip_src" + ipVersion).replace("AND (ip_src" + ipVersion, "AND (ip_dst" + ipVersion).replace("ip_src_priv", "ip_dst_priv") + " GROUP BY ip "
                        + ")xx group by ipa " + orderBy + " " + limite;
            } else if (Talker_Or_Src_Or_Dst.contains("Talkers Sources Bytes")) {
                query = select + " FROM " + job_name + " WHERE (" + ini + " >= " + extremos[0] + " AND " + ini + "<= " + extremos[1] + ") AND (ip_src" + ipVersion + " > 0) " + addProtQuery + " GROUP BY ip "
                        + orderBy + " " + limite;
            } else if (Talker_Or_Src_Or_Dst.contains("Talkers Destinations Bytes")) {
                query = select + " FROM " + job_name + " WHERE (" + ini + " >= " + extremos[0] + " AND " + ini + "<= " + extremos[1] + ") AND (ip_dst" + ipVersion + " > 0) " + addProtQuery.replace("OR ip_dst" + ipVersion, "OR ip_src" + ipVersion).replace("AND (ip_src" + ipVersion, "AND (ip_dst" + ipVersion) + " GROUP BY ip "
                        + orderBy + " " + limite;
            }
            rs = new DataBaseConnection().executeQuery(query);
        } catch (Exception ex) {
            logger.error("ERROR: HostTalkers().getDataSetHostTalkers() " + ex);
        }

        if (rs == null) {
            logger.error("ERROR HostTalkers().getDataSetHostTalkers rs is null");
        }
        return rs;
    }

    //=========================================================================

    private Object createDatasetHostTalkersTable(ObjectMessage objectMessage) {
        ArrayList arrayList = null;
        ResultSet rs = null;

        NumberFormat format = null;
        String[] limites;
        int filaInicial = 0;
        double volGlobalP = 0;


        try {
            String campo = "IP";

            arrayList = new ArrayList();
            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Hostname Talkers")) {
                campo = "Hostname";
            }
            String[] columnNames = {"Num", campo, "volumen bytes", "Packets", "Percentage of use"};

            format = NumberFormat.getInstance();

            arrayList.add(columnNames);

            if(HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")){
                rs = getDataSetHostTalkers(objectMessage, true);
                return new DataBaseConnection().getNumRows(rs);
            } else {
                rs = getDataSetHostTalkers(objectMessage, false);
            }

            DataBaseConnection dbc = new DataBaseConnection();
            int numFilas = dbc.getNumRows(rs) + 1;

            if(!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")){
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }


            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            Object[][] datas = new Object[numFilas][columnNames.length];
            int kk = 0;
            double y = 0;
            double sumTot = 0;
            double sumTotPackets = 0;

            String ip = null;
            HashMap hostnames = new HashMap();

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Hostname Talkers")) {
                while (rs.next()) {
                    ip = new Subnet().inet_ntoa(rs.getString(1));
                    hostnames.put(ip, "");
                }
                hostnames = new ResolveHostname().resolveHostname(hostnames);
            }
            rs.beforeFirst();

            while (rs.next()) {
                ip = new Subnet().inet_ntoa(rs.getString(1));

                datas[kk][0] = filaInicial;
                if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Hostname Talkers")) {
                    ip = (String) hostnames.get(ip);
                }
                datas[kk][1] = ip;
                datas[kk][2] = format.format(Double.valueOf(rs.getString(2)));
                datas[kk][3] = format.format(Double.valueOf(rs.getString(3)));
                y = Double.valueOf(rs.getString(2));
                datas[kk][4] = (y * 100)/volGlobalP;
                sumTot += Double.valueOf(rs.getString(2));
                sumTotPackets += Double.valueOf(rs.getString(3));
                kk++;
                filaInicial++;
            }

            datas[kk][0] = filaInicial;
            datas[kk][1] = "--- TOTAL ---";
            datas[kk][2] = format.format(sumTot);
            datas[kk][3] = format.format(sumTotPackets);
            datas[kk][4] = (sumTot * 100)/volGlobalP;


            arrayList.add(datas);

        } catch (Exception e) {
            logger.error("ERROR: Protocols().IpTalkersBytesTable() ", e);
        }

        return arrayList;
    }

}
