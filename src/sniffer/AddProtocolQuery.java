package sniffer;

import java.net.InetAddress;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.jms.JMSException;
import org.apache.log4j.Logger;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import util.HashMapUtilitie;
import util.Subnet;

/**
 *
 * @author puebla
 */
public class AddProtocolQuery {

    static final Logger logger = Logger.getLogger(Bandwidth.class);

    public String AddProtocolQuery(String job_name, String typeGraph, ObjectMessage objectMessage) {
        String queryWhereNetworkProtocols = "";
        String queryWhereIPProtocols = "";
        String queryWhereTCPProtocols = "";
        String queryWhereUDPProtocols = "";
        String queryWhereIPs = "";
        String queryWhereIpPriv = "";
        String queryWhereIpPubl = "";
        String queryWebUrl = "";
        String whereSubnet = "";
        String whereHostWeb = "";
        String whereToS = "";

        //String selectIPprotocol = "";
        String allWhereCond = "";
        String wherePortNumber = "";

        try {
            allWhereCond = "";

            if (!HashMapUtilitie.getProperties(objectMessage, "ip address").equalsIgnoreCase("not applicable")) {
                queryWhereIPs = getWhereIPs(HashMapUtilitie.getProperties(objectMessage, "ip address").split(","), typeGraph, HashMapUtilitie.getProperties(objectMessage, "view"), false);
            } else if (!HashMapUtilitie.getProperties(objectMessage, "hostname").equalsIgnoreCase("not applicable")) {
                queryWhereIPs = getWhereIPs(HashMapUtilitie.getProperties(objectMessage, "hostname").split(","), typeGraph, HashMapUtilitie.getProperties(objectMessage, "view"), true);
            } else {
                queryWhereIPs = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "websites").equalsIgnoreCase("not applicable")) {
                queryWebUrl = " AND (ip_dst IN (SELECT DISTINCT(IF(port_src > port_dst, ip_dst_v4, ip_src_v4)) FROM " + job_name
                        + " WHERE referer_web IN (" + getINQueryProtocols(HashMapUtilitie.getProperties(objectMessage, "websites").split(",")) + ")))";
            } else {
                queryWebUrl = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "subnet").contains("not applicable")) {
                String[] subnetIpMask = new String[2];
                subnetIpMask[0] = HashMapUtilitie.getProperties(objectMessage, "subnet");
                subnetIpMask[1] = HashMapUtilitie.getProperties(objectMessage, "netmask");
                whereSubnet = getWhereSubnet(subnetIpMask);
            } else {
                whereSubnet = "";
            }
            //HashMapUtilitie.getPropertiesNames(objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "web server hosts").equalsIgnoreCase("not applicable")) {
                whereHostWeb = "AND ((ip_src_v4 IN (SELECT DISTINCT(IF(port_src > port_dst, ip_dst_v4, ip_src_v4)) FROM " + job_name
                        + " WHERE host_web IN (" + getINQueryProtocols(HashMapUtilitie.getProperties(objectMessage, "web server hosts").split(",")) + ")) OR "
                        + " ip_dst_v4 IN (SELECT DISTINCT(IF(port_src > port_dst, ip_dst_v4, ip_src_v4)) FROM " + job_name
                        + " WHERE host_web IN (" + getINQueryProtocols(HashMapUtilitie.getProperties(objectMessage, "web server hosts").split(",")) + "))))";
            } else {
                whereHostWeb = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "type service").equalsIgnoreCase("not applicable")) {
                whereToS = getWhereToS(HashMapUtilitie.getProperties(objectMessage, "type service").split(","));
                //job_name = job_name.replace(prefijo_convers, prefijo_jobs);
                //querySelect = "SUM(bytes)";
                //ini = "seg";
            } else {
                whereToS = "";
                //querySelect = "sum(bytes_src + bytes_dst)";
                //ini = "ini_conv";
            }

            if (HashMapUtilitie.getProperties(objectMessage, "type protocol").equalsIgnoreCase("IP Protocols")
                    && HashMapUtilitie.getProperties(objectMessage, "network protocols").equalsIgnoreCase("not applicable")
                    && HashMapUtilitie.getProperties(objectMessage, "ip protocols").equalsIgnoreCase("not applicable")
                    && HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable")
                    && HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable")) {
                queryWhereNetworkProtocols = " AND (ether_type IN(2048)) ";
            }


            if (!HashMapUtilitie.getProperties(objectMessage, "network protocols").equalsIgnoreCase("not applicable")) {
                queryWhereNetworkProtocols = getWhereProtocols("NetworkProtocols", objectMessage);
            } else {
                queryWhereIPProtocols = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "ip protocols").equalsIgnoreCase("not applicable")) {
                queryWhereIPProtocols = getWhereProtocols("IPProtocols", objectMessage);
            } else {
                queryWhereIPProtocols = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable")) {
                queryWhereTCPProtocols = getWhereProtocols("TCPProtocols", objectMessage);
            } else {
                queryWhereTCPProtocols = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable")) {
                queryWhereUDPProtocols = getWhereProtocols("UDPProtocols", objectMessage);
            } else {
                queryWhereUDPProtocols = "";
            }

            if (!HashMapUtilitie.getProperties(objectMessage, "port number").equalsIgnoreCase("not applicable")) {
                wherePortNumber = getPortNumber(objectMessage);
            } else {
                wherePortNumber = "";
            }

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Private IP Talkers Bytes")) {
                queryWhereIpPriv = " AND (ip_src_priv = 1) ";
            } else {
                queryWhereIpPriv = "";
            }

            if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Public IP Talkers Bytes")) {
                queryWhereIpPubl = " AND (ip_src_priv = 0) ";
            } else {
                queryWhereIpPubl = "";
            }

            allWhereCond = queryWhereIPs + queryWebUrl + whereSubnet + whereHostWeb + whereToS + queryWhereNetworkProtocols + queryWhereIPProtocols + queryWhereTCPProtocols + queryWhereUDPProtocols + wherePortNumber + queryWhereIpPriv + queryWhereIpPubl;

        } catch (Exception e) {
            logger.error("ERROR: AddProtocolQuery().AddProtocolQuery() ", e);
        }
        if (allWhereCond.equals("null")) {
            allWhereCond = "";
        }
        return allWhereCond;
    }

    //=========================================================================
    
    private String getWhereIPs(String ips[], String typeGraph, String select, boolean isHostName) {
        String queryWhereIPs = "";

        try {

            if(isHostName){
                for (int i = 0; i < ips.length; i++) {
                    ips[i] = InetAddress.getByName(ips[i]).getHostAddress();
                }
            }


            if (select.equals("Bandwidth Over Time Source Bits") || typeGraph.equals("Bandwidth Over Time Source Bits") || typeGraph.contains("IP Sources Bytes")) {
                queryWhereIPs = " AND (ip_src_v4 IN (INET_ATON(\"";
                int ipN = 0;
                for (ipN = 0; ipN < ips.length - 1; ipN++) {
                    queryWhereIPs += ips[ipN] + "\"), INET_ATON(\"";
                }
                queryWhereIPs += ips[ipN] + "\")))";
            } else if (select.equals("Bandwidth Over Time Destination Bits") || typeGraph.equals("Bandwidth Over Time Destination Bits") || typeGraph.contains("IP Destination Bytes")) {
                queryWhereIPs = " AND (ip_dst_v4 IN (INET_ATON(\"";
                int ipN = 0;
                for (ipN = 0; ipN < ips.length - 1; ipN++) {
                    queryWhereIPs += ips[ipN] + "\"), INET_ATON(\"";
                }
                queryWhereIPs += ips[ipN] + "\")))";
            } else {

                queryWhereIPs = " AND (ip_src_v4 IN (INET_ATON(\"";
                int ipN = 0;
                for (ipN = 0; ipN < ips.length - 1; ipN++) {
                    queryWhereIPs += ips[ipN] + "\"), INET_ATON(\"";
                }
                queryWhereIPs += ips[ipN] + "\")) OR ip_dst_v4 IN (INET_ATON(\"";

                ipN = 0;
                for (ipN = 0; ipN < ips.length - 1; ipN++) {
                    queryWhereIPs += ips[ipN] + "\"), INET_ATON(\"";
                }
                queryWhereIPs += ips[ipN] + "\")))";
            }

        } catch (Exception ex) {
            logger.error("ERROR: AddProtocolQuery().getWhereIPs: " + ex);
        }

        return queryWhereIPs;
    }

    //=========================================================================
    private String getINQueryProtocols(String[] protocols) {
        String queryIN = null;
        try {
            for (int i = 0; i < protocols.length; i++) {
                if (i == 0) {
                    queryIN = "\"" + protocols[i] + "\"";
                }
                if (i > 0) {
                    queryIN += ",\"" + protocols[i] + "\"";
                }
            }
        } catch (Exception e) {
            logger.error("ERROR AddProtocolQuery().getINQueryProtocols(): " + e);
        }
        return queryIN;
    }

    //========================================================================
    private String getWhereToS(String[] tos) {
        String whereToS = "";
        String concatenaDesconocidos = "";
        String concatenaConocidos = "";

        try {
            whereToS = " AND (tos IN (";

            concatenaDesconocidos = getDSCPunknows(tos);
            concatenaConocidos = getDSCPKnows(tos);

            if (concatenaDesconocidos.length() > 0 && concatenaConocidos.length() > 0) {
                whereToS += concatenaDesconocidos + "," + concatenaConocidos;
            } else {
                whereToS += concatenaDesconocidos + concatenaConocidos;
            }

            whereToS += "))";
        } catch (Exception e) {
            logger.error("ERROR: AddProtocolQuery().getWhereToS():  " + e);
        }
        return whereToS;
    }

    //========================================================================
    private static String getDSCPunknows(String[] tos) {
        int numDesc = 0;
        String[] desconocidos = null;
        String concatenaDesconocidos = "";

        try {
            for (int d = 0; d < tos.length; d++) {
                if (tos[d].contains("Unknown")) {
                    numDesc++;
                }
            }
            desconocidos = new String[numDesc];

            int desco = 0;
            for (int d = 0; d < tos.length; d++) {
                if (tos[d].contains("Unknown-")) {
                    desconocidos[desco] = tos[d].replace("Unknown-", "");
                    desco++;
                }
            }

            if (numDesc > 0) {
                int k = 0;
                for (k = 0; k < desconocidos.length - 1; k++) {
                    concatenaDesconocidos += desconocidos[k] + ",";
                }
                concatenaDesconocidos += desconocidos[k];
            } else {
                concatenaDesconocidos = "";
            }
        } catch (Exception e) {
            logger.error("ERROR:  AddProtocolQuery().getDSCPunknows():  " + e);
        }
        return concatenaDesconocidos;
    }

    //=========================================================================
    private static String getDSCPKnows(String[] tos) {
        String tos2dec = "";
        int posToS = 0;
        int numConocidos = 0;
        String[] tosKnown = null;
        ResultSet rs = null;
        int filasTos = 0;
        String knowToS = "";

        try {
            tos2dec = " SELECT tos_dec FROM type_of_service WHERE dscp_class IN (";


            for (posToS = 0; posToS < tos.length; posToS++) {
                if (!tos[posToS].contains("Unknown-")) {
                    numConocidos++;
                }
            }

            if (numConocidos > 0) {
                tosKnown = new String[numConocidos];

                int conoc = 0;
                for (posToS = 0; posToS < tos.length; posToS++) {
                    if (!tos[posToS].contains("Unknown-")) {
                        tosKnown[conoc] = tos[posToS];
                        conoc++;
                    }
                }

                for (posToS = 0; posToS < tosKnown.length - 1; posToS++) {
                    tos2dec += "\"" + tosKnown[posToS] + "\",";
                }
                tos2dec += "\"" + tosKnown[posToS] + "\")";

                rs = new DataBaseConnection().executeQuery(tos2dec);
                filasTos = new DataBaseConnection().getNumRows(rs);


                for (int i = 0; i < filasTos - 1; i++) {
                    rs.next();
                    knowToS += rs.getString(1) + ",";
                }
                rs.next();
                knowToS += rs.getString(1);

            } else {
                knowToS = "";
            }
        } catch (Exception e) {
            logger.error("ERROR:  AddProtocolQuery().getDSCPKnowns():  " + e);
        }
        return knowToS;
    }

    //=========================================================================
    private String getWhereSubnet(String[] subnetIpMask) {
        String whereSubnet = "";
        try {
            Subnet sub = new Subnet();
            sub.setIPAddress(subnetIpMask[0]);
            sub.setMaskedBits(Integer.valueOf(subnetIpMask[1]));            

            whereSubnet = " AND ((ip_src_v4 BETWEEN " + sub.inet_aton(sub.getMinimumHostAddressRange()) + " AND " + sub.inet_aton(sub.getMaximumHostAddressRange()) + ") "
                    + " OR (ip_dst_v4 BETWEEN " + sub.inet_aton(sub.getMinimumHostAddressRange()) + " AND " + sub.inet_aton(sub.getMaximumHostAddressRange()) + ")) ";
        } catch (Exception e) {
            logger.error("ERROR: AddProtocolQuery().getWhereSubnet():  " + e);
        }

        return whereSubnet;
    }

    //=========================================================================
    private String getQueryUnknowProtocols(String[] protocols, String unknow) {
        String[] protoUnknow = null;
        int numUnknows = 0;
        int np = 0;

        for (int i = 0; i < protocols.length; i++) {
            if (protocols[i].contains("-Unknow")) {
                numUnknows++;
            }
        }


        protoUnknow = new String[numUnknows];


        for (int j = 0; j < protocols.length; j++) {
            if (protocols[j].contains("-Unknown")) {
                protoUnknow[np] = protocols[j].replace("-Unknown", "");
                np++;
            }

        }

        if (np > 0) {
            for (int k = 0; k < protoUnknow.length - 1; k++) {
                unknow += protoUnknow[k] + ",";
            }
            unknow += protoUnknow[protoUnknow.length - 1] + ") ";
            return unknow;
        }
        if (unknow.equalsIgnoreCase(unknow)) {
            unknow = "";
        }

        return "";
    }

    //========================================================================
    private String getWhereQuery(String query, String protoTypeTable, String whereINProto) {
        DataBaseConnection db = null;
        ResultSet rs = null;
        DecimalFormat format = null;

        try {
            format = new DecimalFormat("#############");
            db = new DataBaseConnection();
            rs = db.executeQuery(query);

            int numProtocols = 0;
            numProtocols = db.getNumRows(rs);

            if (numProtocols == 0) {
                return "No aplica";
            }

            rs.next();
            for (int np = 0; np < (numProtocols - 1); np++) {
                whereINProto += format.format(rs.getDouble(1)) + ", ";
                rs.next();
            }
            whereINProto += format.format(rs.getDouble(1)) + "))";



            db.closeResultSet(rs);

        } catch (Exception e) {
            logger.error("Error AddProtocolQuery().getWhereQuery():  rs is null:  " + e);
        } finally {
            db.closeResultSet(rs);
        }
        return whereINProto;
    }

//========================================================================
    private String getWhereQueryTCPoUDPProtocols(String[] protocols) {
        int numProtocols = 0;
        String whereINProto = null;

        try {
            whereINProto = " AND (port_src_name IN (\"";
            numProtocols = protocols.length;

            for (int np = 0; np < (numProtocols - 1); np++) {
                whereINProto += protocols[np] + "\",\"";
            }
            whereINProto += protocols[protocols.length - 1] + "\") OR port_dst_name IN (\"";

            for (int np = 0; np < (numProtocols - 1); np++) {
                whereINProto += protocols[np] + "\",\"";
            }
            whereINProto += protocols[protocols.length - 1] + "\"))";
            System.out.println("whereINProto: " + whereINProto);

        } catch (Exception e) {
            logger.error("Error AddProtocolQuery().getWhereQueryTCPoUDPProtocols():  " + e);
        }

        return whereINProto;
    }

    //=========================================================================
    private String getWhereProtocols(String protocol, ObjectMessage objectMessage) {

        String whereINProto = "";
        String protoTypeTable = "";
        String[] protocols = null;
        String seleccionado = null;
        String query = null;
        String whereP = null;
        String protocolsUnknows = "";

        try {
            seleccionado = HashMapUtilitie.getProperties(objectMessage, "drillDown");

            if (protocol.equalsIgnoreCase("NetworkProtocols") && !HashMapUtilitie.getProperties(objectMessage, "network protocols").equalsIgnoreCase("not applicable")) {
                whereP = "";

                if (HashMapUtilitie.getProperties(objectMessage, "type protocol").equalsIgnoreCase("IP Protocols")
                        && HashMapUtilitie.getProperties(objectMessage, "network protocols").contains("IP,")) {
                    protocols = new String[1];
                    protocols[0] = "IP";
                } else if (HashMapUtilitie.getProperties(objectMessage, "type protocol").equalsIgnoreCase("IP Protocols")
                        && !HashMapUtilitie.getProperties(objectMessage, "network protocols").contains("IP,")) {
                    whereP = " AND (1 = 5 )";
                } else {
                    protocols = HashMapUtilitie.getProperties(objectMessage, "network protocols").split(",");
                }

                if (!whereP.equals(" AND (1 = 5 )")) {
                    query = "SELECT DISTINCT(protocoldec) FROM ethernet_protocols WHERE keyword IN (" + getINQueryProtocols(protocols) + ")";
                    protoTypeTable = "ethernet_protocols";
                    String qunknow = " AND ether_type IN (";
                    protocolsUnknows = getQueryUnknowProtocols(protocols, qunknow);

                    whereINProto = " AND (ether_type IN (";
                    whereP = getWhereQuery(query, protoTypeTable, whereINProto);

                    if (whereP.equalsIgnoreCase("No aplica")) {
                        whereP = protocolsUnknows;
                    } else {
                        if (protocolsUnknows.length() > 1) {
                            whereP = whereP.replace("))", "," + protocolsUnknows.substring(protocolsUnknows.indexOf("(") + 1, protocolsUnknows.length() - 1) + ")");
                        }
                    }
                }
            } else if (protocol.equalsIgnoreCase("IPProtocols") && !HashMapUtilitie.getProperties(objectMessage, "ip protocols").equalsIgnoreCase("not applicable")) {
                protocols = HashMapUtilitie.getProperties(objectMessage, "ip protocols").split(",");
                query = "SELECT DISTINCT(pro_decimal) FROM ip_protocols WHERE keyword IN (" + getINQueryProtocols(protocols) + ")";
                protoTypeTable = "ip_protocols";
                whereINProto = " AND (ip_protocol IN (";
                whereP = getWhereQuery(query, protoTypeTable, whereINProto);
            } else if ((HashMapUtilitie.getProperties(objectMessage, "network protocols").contains("IP") || !HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable")) && protocol.equalsIgnoreCase("TCPProtocols") && (!HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable"))) {
                protocols = HashMapUtilitie.getProperties(objectMessage, "tcp protocols").split(",");
                protoTypeTable = "protocols_tcp_udp";
                //whereINProto = " AND (port IN (";
                whereP = getWhereQueryTCPoUDPProtocols(protocols);
            } else if ((HashMapUtilitie.getProperties(objectMessage, "network protocols").contains("IP") || !HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable")) && protocol.equalsIgnoreCase("UDPProtocols") && (!HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable"))) {
                protocols = HashMapUtilitie.getProperties(objectMessage, "udp protocols").split(",");
                protoTypeTable = "protocols_tcp_udp";
                //whereINProto = " AND (port IN (";
                whereP = getWhereQueryTCPoUDPProtocols(protocols);
            } else //este que sigue es el que hay que corregir 30 enero 2012 // selectMessage.contains("Websites") || selectMessage.contains("Web Server Hosts") ||
            if (seleccionado.equalsIgnoreCase("Type of Service") || seleccionado.equalsIgnoreCase("IP Talkers Bytes")) {
                protoTypeTable = "protocols_tcp_udp";
                if (protocol.equalsIgnoreCase("TCPProtocols") && !HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable")) {
                    protocols = HashMapUtilitie.getProperties(objectMessage, "tcp protocols").split(",");
                } else if (protocol.equalsIgnoreCase("UDPProtocols") && !HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable")) {
                    protocols = HashMapUtilitie.getProperties(objectMessage, "udp protocols").split(",");
                }
                //whereINProto = " AND (port IN (";
                whereP = getWhereQueryTCPoUDPProtocols(protocols);
            } else if (seleccionado.equalsIgnoreCase("Web Server Hosts")) {
                whereP = "";
            } else {
                whereP = " AND (1 = 3 )";
            }

        } catch (Exception e) {
            logger.error("ERROR: AddProtocolQuery().getWhereProtocols(): " + e);
        }

        if (whereP.equalsIgnoreCase("No aplica")) {
            return "AND (1 = 4)";
        }

        return whereP;
    }

    //=========================================================================
    String[] getExtremosLimites(Message message) {
        String[] extremos = {"0", "0"};
        try {

            if (!message.getStringProperty("extra1").equalsIgnoreCase("not applicable")) {
                extremos = message.getStringProperty("extra1").split(",");
                extremos = getExtremos(extremos);
            } else {//estos corregirlos 29 feb 2012
//                String[] rangoJob = Jobs.getRangoJob(message);
//                extremos[0] = rangoJob[0];
//                extremos[1] = rangoJob[1];
            }

        } catch (JMSException ex) {
            logger.error("ERROR: AddProtocolQuery().getExtremosLimites:  " + ex);
        }
        return extremos;
    }

    //=========================================================================
    private String[] getExtremos(String extremos[]) {

        if (extremos == null) {
            logger.error("ERROR FilDataSet().getExtremos(): extremos is null");
        }
        Long muestreo1[] = null;
        DateFormat formatter = null;
        Date date = null;
        Date date1 = null;

        try {

            muestreo1 = new Long[63];

            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            date = (Date) formatter.parse(extremos[0]);
            date1 = (Date) formatter.parse(extremos[1]);

            //muestreo1 = muestreo1((date.getTime() / 1000), (date1.getTime() / 1000));
            extremos = null;
            extremos = new String[2];
            extremos[0] = String.valueOf(date.getTime() / 1000);
            extremos[1] = String.valueOf(date1.getTime() / 1000);
        } catch (Exception e) {
            logger.error("ERROR AddProtocolQuery().getExtremos(): " + e);
//            jmserror.reportErrorToClient(message, e);
        }
        return extremos;
    }

    //=========================================================================
    int getFilaInicial(Message message) {
        String[] limites = null;
        int filaInicial = 0;

        try {
            if (!message.getStringProperty("extra6").equalsIgnoreCase("not applicable")) {
                limites = message.getStringProperty("extra6").split(",");
                filaInicial = Integer.valueOf(limites[0]);
                limites = null;
            } else {
                filaInicial = 0;
            }
        } catch (JMSException ex) {
            logger.error("ERROR: AddProtocolQuery().getFilaInicial(): " + ex);
        }
        return filaInicial;
    }

    //=========================================================================
    private String getPortNumber(ObjectMessage objectMessage) {
        String puertos[] = null;
        String wherePortNumb = null;
        try {
            puertos = HashMapUtilitie.getProperties(objectMessage, "port number").split(",");
            wherePortNumb = " AND (port_src IN (";
            int i = 0;
            for (i = 0; i < puertos.length - 1; i++) {
                wherePortNumb += puertos[i] + ",";
            }
            wherePortNumb += puertos[i] + ") OR port_dst IN (";

            int j = 0;
            for (j = 0; j < puertos.length - 1; j++) {
                wherePortNumb += puertos[j] + ",";
            }

            wherePortNumb += puertos[j] + ")) ";

        } catch (Exception e) {
            logger.error("ERROR AddProtocolQuery().getPortNumber(): " + e);
        }
        return wherePortNumb;
    }
    //=========================================================================
}
