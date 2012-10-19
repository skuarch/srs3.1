package sniffer;

import java.sql.ResultSet;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import util.HashMapUtilitie;
import util.Subnet;
import util.VolumeUtilities;

/**
 *
 * @author puebla
 */
public class ConversationsBytes implements Runnable {

    static final Logger logger = Logger.getLogger(ConversationsBytes.class);
    private ObjectMessage objectMessage = null;
    private static final String TCP = "tcp";
    private static final String UDP = "udp";
    private static final String TCPoUDP = "tcp or udp";
    String ipVersion = null;

    //=========================================================================
    public ConversationsBytes(ObjectMessage objectMessage, String ipVersion) {
        this.objectMessage = objectMessage;
        this.ipVersion = ipVersion;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            conversationsBytesDataset();
        } catch (Exception e) {
            logger.error("ERROR ConversationsBytes().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================
    private void conversationsBytesDataset() {
        Object data = null;
        try {
            data = createDataSetConversationsBytes(this.objectMessage);
            new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);
        } catch (Exception e) {
            logger.error("ERROR: ConversationsBytes().conversationsBytesDataset(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread ConversationsBytes()");
        }
    }

    //=========================================================================
    private Object createDataSetConversationsBytes(ObjectMessage objectMessage) {
        ArrayList arrayList = null;
        ResultSet rs = null;

        NumberFormat nf = null;
        double y = 0;
        double volGlobalP = 0;
        String[] limites;
        int filaInicial = 1;
        String tipo = "";

        try {
            nf = NumberFormat.getInstance();
            arrayList = new ArrayList();
            String[] columnNames = {"Num", "Source IP", "Source Port", "Destination IP", "Destination Port", "Source Bytes", "Destination Bytes", "Total Bytes", "Percentage of use", "Starts", "Ends"};

            arrayList.add(columnNames);

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("TCP")) {
                tipo = TCP;
            } else if (HashMapUtilitie.getProperties(objectMessage, "view").contains("UDP")) {
                tipo = UDP;
            } else {
                tipo = TCPoUDP;
            }

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Rows")) {
                int numRows = 0;
                rs = getDataSetConversations(objectMessage, tipo, true);
                numRows = new DataBaseConnection().getNumRows(rs);
                rs = null;
                return numRows;
            } else {
                rs = getDataSetConversations(objectMessage, tipo, false);
            }

            DataBaseConnection dbc = new DataBaseConnection();
            int numFilas = dbc.getNumRows(rs);

            Object[][] datas = new Object[numFilas][12];
            volGlobalP = new VolumeUtilities().getVolumenGlobalPeriodo(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "limit").equalsIgnoreCase("not applicable")) {
                limites = HashMapUtilitie.getProperties(objectMessage, "limit").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            }

            int kk = 0;

            String DATE_FORMAT_NOW = "yyyy-MM-dd  HH:mm:ss";
            SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT_NOW);
            while (rs.next()) {
                if ((rs.getString(10).equalsIgnoreCase("1") && rs.getString(11).equalsIgnoreCase("1"))
                        || (rs.getString(10).equalsIgnoreCase("0") && rs.getString(11).equalsIgnoreCase("0"))) {
                    if (rs.getInt(2) >= rs.getInt(4)) {
                        datas[kk][0] = filaInicial;
                        datas[kk][1] = new Subnet().inet_ntoa(rs.getString(1));
                        datas[kk][2] = rs.getInt(2);
                        datas[kk][3] = new Subnet().inet_ntoa(rs.getString(3));
                        datas[kk][4] = rs.getInt(4);
                        datas[kk][5] = nf.format(Long.parseLong(rs.getString(5)));
                        datas[kk][6] = nf.format(Long.parseLong(rs.getString(6)));
                        datas[kk][7] = nf.format(Long.parseLong(rs.getString(7)));
                        y = Double.valueOf((Long.parseLong(rs.getString(7))));
                        datas[kk][8] = (y * 100) / volGlobalP;
                        datas[kk][9] = df.format(rs.getTimestamp(8));
                        datas[kk][10] = df.format(rs.getTimestamp(9));
                    } else {
                        datas[kk][0] = filaInicial;
                        datas[kk][1] = new Subnet().inet_ntoa(rs.getString(3));
                        datas[kk][2] = rs.getInt(4);
                        datas[kk][3] = new Subnet().inet_ntoa(rs.getString(1));
                        datas[kk][4] = rs.getInt(2);
                        datas[kk][5] = nf.format(Long.parseLong(rs.getString(6)));
                        datas[kk][6] = nf.format(Long.parseLong(rs.getString(5)));
                        datas[kk][7] = nf.format(Long.parseLong(rs.getString(7)));
                        y = Double.valueOf((Long.parseLong(rs.getString(7))));
                        datas[kk][8] = (y * 100) / volGlobalP;
                        datas[kk][9] = df.format(rs.getTimestamp(8));
                        datas[kk][10] = df.format(rs.getTimestamp(9));
                    }
                } else if (rs.getString(10).equalsIgnoreCase("1")) {
                    datas[kk][0] = filaInicial;
                    datas[kk][1] = new Subnet().inet_ntoa(rs.getString(1));
                    datas[kk][2] = rs.getInt(2);
                    datas[kk][3] = new Subnet().inet_ntoa(rs.getString(3));
                    datas[kk][4] = rs.getInt(4);
                    datas[kk][5] = nf.format(Long.parseLong(rs.getString(5)));
                    datas[kk][6] = nf.format(Long.parseLong(rs.getString(6)));
                    datas[kk][7] = nf.format(Long.parseLong(rs.getString(7)));
                    y = Double.valueOf((Long.parseLong(rs.getString(7))));
                    datas[kk][8] = (y * 100) / volGlobalP;
                    datas[kk][9] = df.format(rs.getTimestamp(8));
                    datas[kk][10] = df.format(rs.getTimestamp(9));
                } else {
                    datas[kk][0] = filaInicial;
                    datas[kk][1] = new Subnet().inet_ntoa(rs.getString(3));
                    datas[kk][2] = rs.getInt(4);
                    datas[kk][3] = new Subnet().inet_ntoa(rs.getString(1));
                    datas[kk][4] = rs.getInt(2);
                    datas[kk][5] = nf.format(Long.parseLong(rs.getString(6)));
                    datas[kk][6] = nf.format(Long.parseLong(rs.getString(5)));
                    datas[kk][7] = nf.format(Long.parseLong(rs.getString(7)));
                    y = Double.valueOf((Long.parseLong(rs.getString(7))));
                    datas[kk][8] = (y * 100) / volGlobalP;
                    datas[kk][9] = df.format(rs.getTimestamp(8));
                    datas[kk][10] = df.format(rs.getTimestamp(9));
                }
                kk++;
                filaInicial++;
            }

            arrayList.add(datas);
            dbc.closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR: IPConversations().Conversations() ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in createDataSetConversationsBytes()");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }

        return arrayList;
    }

    //=========================================================================
    private ResultSet getDataSetConversations(ObjectMessage objectMessage, String tipo, boolean countRows) {
        ResultSet rs = null;
        String job_name = "";
        String limite = "";
        String limites[] = {"0", "0"};
        String addProtQuery = "";
        AddProtocolQuery addPQ = null;
        String orderBy = "";
        String select = "";




        try {
            addPQ = new AddProtocolQuery();
            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];

            if (tipo.equals("tcp or udp")) {
                tipo = " AND (ip_protocol IN (6, 17)) ";
            } else if (tipo.equals("tcp")) {
                tipo = " AND (ip_protocol = 6) ";
            } else if (tipo.equals("udp")) {
                tipo = " AND (ip_protocol = 17) ";
            }

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

            if (countRows) {
                select = "SELECT COUNT(*) ";
                orderBy = "";
            } else {
                select = "SELECT ip_a, port_a, ip_b, port_b, SUM(bytes_a), SUM(bytes_b), SUM(bytes_a + bytes_b) vt, FROM_UNIXTIME(MIN(iniConv)) ini, FROM_UNIXTIME(MAX(finConv)) fin, priv_a, priv_b ";
                orderBy = " ORDER BY vt DESC ";
            }

            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name, "", objectMessage);

            rs = new DataBaseConnection().executeQuery(select
                    + " FROM ("
                    + " SELECT (ip_src" + ipVersion + ") ip_a, port_src port_a, (ip_dst" + ipVersion + ") ip_b, port_dst port_b, SUM(bytes) bytes_a, 0 bytes_b, id_conex, ip_protocol, flujo, MIN(ini_conv) iniConv, MAX(fin_conv) finConv, ip_src_priv priv_a, ip_dst_priv priv_b "
                    + " FROM " + job_name
                    + " WHERE (ini_conv >= " + extremos[0] + " AND ini_conv<= " + extremos[1] + ")"
                    + addProtQuery + tipo + " AND flujo = 0 AND port_src !=-11"
                    + " GROUP BY ip_src" + ipVersion + ", port_src, ip_dst" + ipVersion + ", port_dst, id_conex,flujo,ip_protocol "
                    + " UNION ALL "
                    + " SELECT (ip_dst" + ipVersion + ") ip_a, port_dst port_a, (ip_src" + ipVersion + ") ip_b, port_src port_b, 0 bytes_a, SUM(bytes) bytes_b, id_conex, ip_protocol, flujo, MIN(ini_conv) iniConv, MAX(fin_conv) finConv, ip_dst_priv priv_a, ip_src_priv priv_b "
                    + " FROM " + job_name
                    + " WHERE (ini_conv >= " + extremos[0] + " AND ini_conv<= " + extremos[1] + ")"
                    + addProtQuery + tipo + " AND flujo = 1 AND port_dst !=-11"
                    + " GROUP BY ip_src" + ipVersion + ", port_src, ip_dst" + ipVersion + ", port_dst, id_conex,flujo,ip_protocol "
                    + ")xx GROUP BY ip_a, port_a, ip_b, port_b,id_conex, ip_protocol "
                    + orderBy + limite);

        } catch (Exception e) {
            logger.error("ERROR FillDataSet().getDataSetIpFrames" + e);
            new JMSProccessor().sendError(objectMessage, "ERROR IN getDataSetConversations()");
        }

        if (rs == null) {
            logger.error("ERROR FillDataSet().getDataSetIpFrames rs is null");
            new JMSProccessor().sendError(objectMessage, "ERROR IN getDataSetConversations() resultSet is null");
        }

        return rs;
    }
}
