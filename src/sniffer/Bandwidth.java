package sniffer;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import util.HashMapUtilitie;

/**
 *
 * @author puebla
 */
public class Bandwidth implements Runnable {

    static final Logger logger = Logger.getLogger(Bandwidth.class);
    private ObjectMessage objectMessage = null;

    //=========================================================================
    public Bandwidth(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //=========================================================================
    @Override
    public void run() {
        try {
            bandwidthOverTime();
        } catch (Exception e) {
            logger.error("ERROR Bandwidth().run(): " + e);
            new JMSProccessor().sendError(objectMessage, "Imposible get dataset");
        }
    }

    //=========================================================================
    public void bandwidthOverTime() {

        try {
            Thread t = new Thread() {

                @Override
                public void run() {
                    Object data = null;

                    try {
                        String extremos[] = Jobs.getJobExtremos(objectMessage);

                        if (HashMapUtilitie.getProperties(objectMessage, "is table").contains("true")) {
                            data = getDataSetBandidthTable(objectMessage, extremos);
                        } else {
                            data = createDatasetBandwidthOverTime(objectMessage, extremos);//mando los puntos extremos para el zoom
                        }

                        new JMSProccessor().send("response " + HashMapUtilitie.getProperties(objectMessage, "view"), objectMessage, data);

                    } catch (Exception e) {
                        logger.error("ERROR: bandwidthOverTime: ", e);
                        new JMSProccessor().sendError(objectMessage, "ERROR in bandwidthOverTime");
                    }
                }
            };
            t.setName("bandwidth Over Time");
            t.start();
        } catch (Exception e) {
            logger.error("ERROR: Bandwidth().bandwidthOverTimeBits(): ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in thread bandwidthOverTime");
        }
    }
    //=========================================================================

    Object getDataSetBandidthTable(ObjectMessage objectMessage, String extremos[]) {
        ResultSet rs = null;
        Timestamp timestamp = null;
        String job_name = "";
        String query = null;
        double deltaTime = 60;
        Object[][] datas = null;
        String[] columnNames = null;
        DateFormat formatter = null;
        NumberFormat format = null;
        ArrayList arrayList = null;
        String typeGraph = "";

        try {
            typeGraph = HashMapUtilitie.getProperties(objectMessage, "view");
            arrayList = new ArrayList();
            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];
            query = getQueryDataSetBW(objectMessage, typeGraph, job_name, extremos);
            rs = new DataBaseConnection().executeQuery(query);

            if (typeGraph.contains("Bandwidth Over Time Source and Destination Bits Table")
                    || (typeGraph.equalsIgnoreCase("Bandwidth Over Time Source and Destination Bits") && HashMapUtilitie.getProperties(objectMessage, "is table").equals("true"))) {
                datas = new Object[new DataBaseConnection().getNumRows(rs)][3];
                columnNames = new String[3];
                columnNames[0] = "Date";
                columnNames[1] = "Bandwidth source (Bits/s)";
                columnNames[2] = "Bandwidth destination (Bits/s)";
            } else {
                String bitsBytes = "";
                datas = new Object[new DataBaseConnection().getNumRows(rs)][2];
                columnNames = new String[2];
                columnNames[0] = "Date";

                if (typeGraph.contains("Bits")) {
                    bitsBytes = "Bits/s";
                } else {
                    bitsBytes = "Bytes/s";
                }
                columnNames[1] = "Bandwidth (" + bitsBytes + ")";
            }


            formatter = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
            format = NumberFormat.getInstance();
            int i = 0;
            while (rs.next()) {
                timestamp = new java.sql.Timestamp(rs.getLong(1) * 1000);
                if (typeGraph.contains("Bandwidth Over Time Source and Destination Bits")) {
                    deltaTime = getDeltaTime(rs.getDouble(4), objectMessage);
                    datas[i][0] = formatter.format((java.util.Date) timestamp);
                    datas[i][1] = format.format((int) ((rs.getDouble(2) * 8) / deltaTime));
                    datas[i][2] = format.format((int) ((rs.getDouble(3) * 8) / deltaTime));
                } else if (typeGraph.contains("Bits")) {
                    deltaTime = getDeltaTime(rs.getDouble(3), objectMessage);
                    datas[i][0] = formatter.format((java.util.Date) timestamp);
                    datas[i][1] = format.format((int) ((rs.getDouble(2) * 8) / deltaTime));
                } else if (typeGraph.contains("Bytes")) {
                    deltaTime = getDeltaTime(rs.getDouble(3), objectMessage);

                    datas[i][0] = formatter.format((java.util.Date) timestamp);
                    datas[i][1] = format.format((int) ((rs.getDouble(2)) / deltaTime));
                }
                i++;
            }

            arrayList.add(columnNames);
            arrayList.add(datas);

            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception ex) {
            logger.error("ERROR: Bandwidth().getDataSetBandidthTable() " + ex);
            new JMSProccessor().sendError(objectMessage, "ERROR: Bandwidth().getDataSetBandidthTable()");
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }

        return arrayList;
    }

    //=========================================================================
    static XYDataset createDatasetBandwidthOverTime(ObjectMessage objectMessage, String extremos[]) {
        TimeSeries bwt = null;
        TimeSeries bwt1 = null;
        TimeSeriesCollection dataset = null;
        String typeGraph = "";


        try {
            typeGraph = HashMapUtilitie.getProperties(objectMessage, "view");

            if (typeGraph.equalsIgnoreCase("Bandwidth Over Time Source and Destination Bits")) {
                bwt = getBandwidthOverTimeSeries(objectMessage, extremos, "Bandwidth Over Time Source Bits");
                bwt1 = getBandwidthOverTimeSeries(objectMessage, extremos, "Bandwidth Over Time Destination Bits");

                dataset = new TimeSeriesCollection();
                dataset.addSeries(bwt);
                dataset.addSeries(bwt1);
            } else {
                bwt = getBandwidthOverTimeSeries(objectMessage, extremos, typeGraph);
                dataset = new TimeSeriesCollection();
                dataset.addSeries(bwt);
            }

        } catch (Exception e) {
            logger.error("ERROR: Bandwidth().createDatasetBandwidthOverTime() ", e);
            new JMSProccessor().sendError(objectMessage, "ERROR in Bandwidth dataset is null");
        }

        if (dataset == null) {
            logger.error("ERROR Bandwidth().createDatasetBandwidthOverTime() :  dataset is null");
        }

        return dataset;
    }

    //=========================================================================
    static TimeSeries getBandwidthOverTimeSeries(ObjectMessage objectMessage, String extremos[], String typeGraph) {
        ResultSet rs = null;
        Timestamp timestamp = null;
        String job_name = "";
        String query = null;
        TimeSeries timeSeries = null;
        double deltaTime = 0;



        try {
            timeSeries = new TimeSeries(typeGraph);
            job_name = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[0];
            query = getQueryDataSetBW(objectMessage, typeGraph, job_name, extremos);
            rs = new DataBaseConnection().executeQuery(query);
            deltaTime = 60;

            while (rs.next()) {
                timestamp = new java.sql.Timestamp(rs.getLong(1) * 1000);

                if (typeGraph.endsWith("Bits/s") || typeGraph.endsWith("Bits")) {

                    if (typeGraph.equalsIgnoreCase("Bandwidth Over Time Destination Bits") && (!HashMapUtilitie.getProperties(objectMessage, "web server hosts").equalsIgnoreCase("not applicable"))) {
                        timeSeries.addOrUpdate(new Millisecond(timestamp), ((rs.getDouble(3) * 8) / deltaTime));
                    } else {
                        timeSeries.addOrUpdate(new Millisecond(timestamp), ((rs.getDouble(2) * 8) / deltaTime));
                    }
                } else if (typeGraph.endsWith("Bytes")) {
                    timeSeries.addOrUpdate(new Millisecond(timestamp), ((rs.getDouble(2)) / deltaTime));
                }
            }

            new DataBaseConnection().closeResultSet(rs);
        } catch (Exception e) {
            logger.error("ERROR Bandwidth().getBandwidthOverTimeSeries(): al llenar datos grafica: " + e.getMessage());
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }

        if (timeSeries.isEmpty()) {
            timestamp = new java.sql.Timestamp(Long.valueOf(extremos[0]) * 1000);
            timeSeries.addOrUpdate(new Millisecond(timestamp), 0);
            logger.error("ERROR Bandwidth().getBandwidthOverTimeSeries(): t1 esta vacio");
        }
        return timeSeries;
    }

    //========================================================================
    private static String getQueryDataSetBW(ObjectMessage objectMessage, String typeGraph, String job_name, String extremos[]) {
        String query = null;
        String querySelect = null;
        String queryAyuda = "(1 = 1)";
        String queryWhereFlujo = "";
        //boolean usarHeaders = false;
        String addProtQuery = "";

        String tabla = "";

        try {

            //detalleSeg = Long.parseLong(gp.getProperties("bandwidth.rango.segundos.detalle"));
            //usarHeaders = Boolean.parseBoolean(gp.getProperties("database.use.headers"));
            tabla = (new Jobs(objectMessage).getJobName2Use(HashMapUtilitie.getProperties(objectMessage, "job")))[1];


            if (typeGraph.contains("Bandwidth Over Time Source Bits")) {
                querySelect = "SUM(bytes_src)";
                queryWhereFlujo = " AND (flujo = 0)";
            } else if (typeGraph.contains("Bandwidth Over Time Destination Bits")) {
                querySelect = "SUM(bytes_dst)";
                queryWhereFlujo = " AND (flujo = 1)";
            } else if (HashMapUtilitie.getProperties(objectMessage, "view").contains("Bandwidth Over Time Source and Destination Bits")) {
                querySelect = "SUM(bytes_src), SUM(bytes_dst)";
            } else {
                querySelect = "SUM(bytes_src + bytes_dst)";
            }


            addProtQuery = new AddProtocolQuery().AddProtocolQuery(job_name, typeGraph, objectMessage);

            if (!HashMapUtilitie.getProperties(objectMessage, "network protocols").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "ip protocols").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "tcp protocols").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "udp protocols").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "ip address").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "websites").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "netmask").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "subnet").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "web server hosts").contains("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "type service").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "port number").equalsIgnoreCase("not applicable")
                    || !HashMapUtilitie.getProperties(objectMessage, "hostname").equalsIgnoreCase("not applicable")) {
                if (typeGraph.equals("Bandwidth Over Time Source Bits")) {
                    queryWhereFlujo = " AND (flujo = 0)";
                } else if (typeGraph.equals("Bandwidth Over Time Destination Bits")) {
                    queryWhereFlujo = " AND (flujo = 1)";
                } else if (typeGraph.equals("Bandwidth Over Time Source and Destination Bits")) {
                    queryWhereFlujo = " ";
                }

                querySelect = "bytes";
                query = "SELECT ini_conv as fecha, SUM(" + querySelect + ") vol, (fin_conv - ini_conv) dura_conv FROM " + job_name.replace(tabla, "conv") + " WHERE (ini_conv >= " + extremos[0] + " AND fin_conv <= " + extremos[1] + ") AND" + " (" + queryAyuda + addProtQuery + queryWhereFlujo + ") GROUP BY DAY(FROM_UNIXTIME(ini_conv)), HOUR(FROM_UNIXTIME(ini_conv)),MINUTE(FROM_UNIXTIME(ini_conv)) ORDER BY fecha ASC";
            } else {
                query = "SELECT seg as fecha, " + querySelect + " vol, 60 FROM " + job_name.replace(tabla, "vol_seg") + " WHERE (seg >= " + extremos[0] + " AND seg <= " + extremos[1] + ") GROUP BY DAY(FROM_UNIXTIME(seg)),HOUR(FROM_UNIXTIME(seg)),MINUTE(FROM_UNIXTIME(seg)) ORDER BY fecha ASC";
            }

        } catch (Exception e) {
            logger.error("ERROR: Bandwidth().getQueryDataSetBW(): " + e);
            new JMSProccessor().sendError(objectMessage, "ERROR IN BW QUERY");
        }

        return query;
    }

    //========================================================================
    static double getDeltaTime(double dt, ObjectMessage objectMessage) {
        if (dt == 0) {
            dt = 1;
        }
        return dt;
    }
}
