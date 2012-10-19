/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package sniffer;


//import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.CachedRowSetImpl;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import util.GetProperties;

/**
 * connection with Monet database.
 * @author skuarch
 */
public class DataBaseConnection {

    static final Logger logger = Logger.getLogger(DataBaseConnection.class);

    //=========================================================================
    /**
     * Construct, by create a instance.
     */
    public DataBaseConnection() {
        PropertyConfigurator.configure("configuration/log.properties");
    } // end DataBaseConnection

    //=========================================================================
    /**
     * Connection with database
     * @return Connection
     */
    public Connection getConnection() {

        Connection connection = null;
        GetProperties gp = null;
        String user = null;
        String password = null;
        String server = null;
        String databaseClass = null;
        String databaseName = null;
        String jdbc = null;

        try {

            gp = new GetProperties("configuration/configuration.properties");

            user = gp.getProperties("database.user");
            password = gp.getProperties("database.password");
            server = gp.getProperties("database.server");
            databaseClass = gp.getProperties("database.class");
            databaseName = gp.getProperties("database.name");
            jdbc = gp.getProperties("database.jdbc");
            jdbc = jdbc + server + "/" + databaseName;

            Class.forName(databaseClass);
            connection = DriverManager.getConnection(jdbc, user, password);

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().getConnection() ", e);
        } finally {
            gp = null;
        }

        if (connection == null) {
            logger.error("ERROR: DataBaseConnection(),getConnection() returning null");
        }

        return connection;

    } // end getConnection

    //=========================================================================
    /**
     *  execute any sql sentence and return a resultset.
     * @param sql String
     * @return ResultSet
     */
    public ResultSet executeQuery(String sql) {
        if (sql == null || sql.equalsIgnoreCase("")) {
            logger.error("ERROR: DataBaseConnection().executeQuery().sql is null or empty");
        }

        ResultSet rs = null;
        Connection connection = null;
        Statement st = null;
//        CachedRowSet crs = null;

        try {

            connection = getConnection();

            if (connection == null) {
                logger.error("ERROR: DataBaseConnection().executeQuery().connection is null");
            }

            st = (Statement) connection.createStatement();
            rs = st.executeQuery(sql);

            //resultset is null ?
            if (rs == null) {
                logger.error("ERROR: DataBaseConnection().executeQuery().rs is null ");
            }

//            crs = new CachedRowSetImpl();
//            crs.populate(rs);

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().executeQuery() ", e);
        } finally {
//            closeConnection(connection);
//            closeResultSet(rs);
//            closeStatement(st);
        }

//        return crs;
        return rs;

    } // end executQuery


    //=========================================================================
     public ResultSet executeQueryWhitCache(String sql) {
        if (sql == null || sql.equalsIgnoreCase("")) {
            logger.error("ERROR: DataBaseConnection().executeQuery().sql is null or empty");
        }

        ResultSet rs = null;
        Connection connection = null;
        Statement st = null;
        CachedRowSet crs = null;

        try {

            connection = getConnection();

            if (connection == null) {
                logger.error("ERROR: DataBaseConnection().executeQuery().connection is null");
            }

            st = (Statement) connection.createStatement();
            rs = st.executeQuery(sql);

            //resultset is null ?
            if (rs == null) {
                logger.error("ERROR: DataBaseConnection().executeQuery().rs is null ");
            }

            crs = new CachedRowSetImpl();
            crs.populate(rs);

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().executeQuery() ", e);
        } finally {
            closeConnection(connection);
            closeResultSet(rs);
            closeStatement(st);
        }

        return crs;
//        return rs;

    } // end executQuery

    //=========================================================================
    /**
     * execute updates in the database.
     * @param sql String
     */
    public void update(String sql) {

        if (sql.equals("")) {
            logger.error("ERROR: DataBaseConnection().update().sql is null or empty");
        }

        Connection connection = null;
        Statement st = null;

        try {

            connection = getConnection();
            st = connection.createStatement();
            st.executeUpdate(sql);

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().update() " + e);
        } finally {
            closeConnection(connection);
            closeStatement(st);
        }

    } //end update

    //=========================================================================
    public int getNumRows(ResultSet rs) {

        if (rs == null) {
            logger.error("ERROR: DataBaseConnection().getNumRows().rs is null ");
        }

        int numRows = 0;

        try {

            rs.last();
            numRows = rs.getRow();
            rs.beforeFirst();

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().getNumRows() " + e);
        }
        return numRows;
    }

    //=========================================================================
    /**
     * close connection, if is able
     * @param connection
     */
    public void closeConnection(Connection connection) {

        try {

            if (connection != null) {
                connection.close();
            }

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().closeConnection() " + e);
        } finally {
            connection = null;
        }

    } //end closeConnection

    //=========================================================================
    /**
     * close statement if is able.
     * @param statement
     */
    public void closeStatement(Statement statement) {

        try {

            if (statement != null) {
                statement.close();
            }

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().closeStatement() " + e);
        } finally {
            statement = null;
        }

    } //end closeStatement

    //=========================================================================
    /**
     * close resultset if is able.
     * @param resultSet
     */
    public void closeResultSet(ResultSet resultSet) {

        try {

            if (resultSet != null) {
                resultSet.close();
            }

        } catch (Exception e) {
            logger.error("ERROR: DataBaseConnection().closeResultSet() " + e);
        } finally {
            resultSet = null;
        }

    } //end closeResultSet
} // end class

