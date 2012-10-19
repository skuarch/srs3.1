package sniffer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.jms.ObjectMessage;
import jms.JMSProccessor;
import org.apache.log4j.Logger;
import util.GetProperties;
import util.HashMapUtilitie;

/**
 *
 * @author skuarch
 */
public class Jobs implements Runnable {

    private static final Logger logger = Logger.getLogger(Jobs.class);
    private ObjectMessage objectMessage = null;

    //==========================================================================
    public Jobs(ObjectMessage objectMessage) {
        this.objectMessage = objectMessage;
    }

    //==========================================================================
    @Override
    public void run() {
        try {
            if (objectMessage.getStringProperty("select").equalsIgnoreCase("getJobs")) {
                responseJobs();
            }
        } catch (Exception e) {
            logger.error("run ", e);
            new JMSProccessor().sendError(objectMessage, e.getMessage());
        }

    } // end run    

    //==========================================================================
    private void responseJobs() {

        String[] jobs = null;

        try {
            jobs = getJobs();
            new JMSProccessor().send("response jobs", objectMessage, jobs);
        } catch (Exception e) {
            logger.error("responseJobs", e);
            new JMSProccessor().sendError(objectMessage, e.getMessage());
        }
    }

    //=========================================================================
    public String[] getJobs() {
        String queryGetJobs = null;
        String jobs[] = null;
        ResultSet rs = null;
        DataBaseConnection db = null;
        int idJob = 0;
        GetProperties gp = null;
        String prefijo = "";
        String tabla = "";


        try {
            db = new DataBaseConnection();            
            gp = new GetProperties("configuration/configuration.properties");
            if (gp.getProperties("database.use.headers").equalsIgnoreCase("true")) {
                prefijo = "database.table.headers.prefijo";
                tabla = gp.getProperties("database.table.headers.prefijo");
            } else {
                prefijo = "database.table.convers.prefijo";
                tabla = gp.getProperties("database.table.convers.prefijo");
            }

            queryGetJobs = "SELECT table_name "
                    + " FROM information_schema.TABLES    "
                    + "WHERE table_type = 'BASE TABLE' AND table_schema = '" + gp.getProperties("database.name")
                    + "' AND table_name LIKE \"" + gp.getProperties(prefijo) + "%\""
                    + " ORDER BY CREATE_TIME DESC";
            rs = db.executeQuery(queryGetJobs);

            jobs = new String[db.getNumRows(rs)];

            idJob = 0;
            while (rs.next()) {
                jobs[idJob] = "job " + (rs.getString(1).replace(tabla, "")).replace("_", " ");
                idJob++;
            }
            db.closeResultSet(rs);

            db = null;
            gp = null;
        } catch (Exception e) {
            logger.error("ERROR: Jobs().getJobs() ", e);
            new JMSProccessor().sendError(objectMessage, "No jobs");
        }
        return jobs;
    }

    //=========================================================================
    public String getJobName(String jobName) {
        String job_name = null;
        GetProperties gp = null;
        String tabla = "";

        try {
            gp = new GetProperties("configuration/configuration.properties");

            if (gp.getProperties("database.use.headers").equalsIgnoreCase("true")) {
                tabla = gp.getProperties("database.table.headers.prefijo");
            } else {
                tabla = gp.getProperties("database.table.convers.prefijo");
            }
            job_name = (jobName.replace(tabla, "")).replace("_", " ");
        } catch (Exception ex) {
            logger.error("ERROR DataSetBandwidth.getBandwidthSeries(): al obtener el job_name: " + ex);
        }

        if (job_name == null) {
            logger.error("ERROR Jobs().getJobName job_name is null");
            new JMSProccessor().sendError(objectMessage, "Error job names");
        }

        return job_name;
    }

//=========================================================================
    static String[] getRangoJob(String job_name) {
        ResultSet rs = null;
        String rango[] = {"0", "0"};

        try {
            job_name = job_name.replaceAll(" ", "_");
            rs = new DataBaseConnection().executeQuery("select min(seg),max(seg) from " + job_name.replace("job_", "vol_seg") + ";");
            rs.next();
            rango[0] = Long.toString(rs.getLong(1));
            rango[1] = Long.toString(rs.getLong(2));

            new DataBaseConnection().closeResultSet(rs);
        } catch (SQLException ex) {
            logger.error("ERROR Jobs().getRangoJob(): getStringProperty()" + ex);
        } finally {
            new DataBaseConnection().closeResultSet(rs);
        }

        if (rango == null || rango.length < 2) {
            logger.error("ERROR: Jobs().getRangoJob() rango = NULL o incompleto");
        }

        return rango;
    }

    //=========================================================================
    public String[] getJobName2Use(String jobName) {
        String job_nameTabla[] = null;
        String tabla = "";
        GetProperties gp = null;


        try {
            job_nameTabla = new String[2];
            gp = new GetProperties("configuration/configuration.properties");
            if (gp.getProperties("database.use.headers").equalsIgnoreCase("true")) {
                tabla = gp.getProperties("database.table.headers.prefijo");
            } else {
                tabla = gp.getProperties("database.table.convers.prefijo");
            }

            job_nameTabla[0] = (jobName.replace("job ", tabla)).replace(" ", "_");
            job_nameTabla[1] = tabla;
        } catch (Exception e) {
            logger.error("ERROR Jobs().getJobName2Use():  " + e);
        }

        return job_nameTabla;
    }
    

    //========================================================================
    public static String[] getJobExtremos(ObjectMessage objectMessage) {
        DateFormat formatter = null;
        String extremos[] = {"0","0"};

        try {
            if (!HashMapUtilitie.getProperties(objectMessage, "dates").equalsIgnoreCase("not applicable")) {//i.e zoom
                extremos = HashMapUtilitie.getProperties(objectMessage, "dates").split(",");

                Date date;
                Date date1;
                formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                date = (Date) formatter.parse(extremos[0]);
                date1 = (Date) formatter.parse(extremos[1]);

                extremos[0] = String.valueOf(date.getTime() / 1000);
                extremos[1] = String.valueOf(date1.getTime() / 1000);

            } else {
                String rangoJob[] = Jobs.getRangoJob(HashMapUtilitie.getProperties(objectMessage, "job"));//jobName

                extremos[0] = rangoJob[0];
                extremos[1] = rangoJob[1];
            }
        } catch (Exception e) {
            logger.error("ERROR al obtener las fechas del job: " + e);
        }

        return extremos;
    }
}
