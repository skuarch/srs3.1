/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package sniffer;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 *
 * @author Julio Puebla
 */
class MatrixProtocols {

    static final Logger logger = Logger.getLogger(MatrixProtocols.class);


    //=========================================================================

    public String[][] getMatrixProtocols(ResultSet rs, String query, boolean countRows) {

        if (rs == null || query ==null) {
            logger.error("ERROR: MatrixProtocols().MatrixProtocols: resultSet or query is null");
            return null;
        }

        int numProtocols = 0;
        String[][] matrizProtocols = null;
        ResultSet protoNameRS = null;
        String protoName = null;
        int i = 0;
        DataBaseConnection db = new DataBaseConnection();

        try {
            numProtocols = db.getNumRows(rs);
            matrizProtocols = new String[numProtocols][3];
            
            while (rs.next()) {
                    protoNameRS = db.executeQueryWhitCache(query + rs.getString(1));
                    protoNameRS.next();
                    if (protoNameRS.getRow() == 0) {
                        protoName = rs.getString(1) + "-Unknown";
                    } else {
                        if (countRows) {
                            if (rs.getString(1).equals("-1111")) {
                                protoName = protoNameRS.getString(1);
                            } else {
                                protoName = rs.getString(1) + "-" + protoNameRS.getString(1);
                            }
                        } else {
                            protoName = protoNameRS.getString(1);
                        }

                    }
                    matrizProtocols[i][0] = rs.getString(1);
                    matrizProtocols[i][1] = rs.getString(2);
                    matrizProtocols[i][2] = protoName;
                    i++;
                    protoNameRS.close();
            }
            db.closeResultSet(rs);
            db.closeResultSet(protoNameRS);
        } catch (SQLException ex) {
            logger.error("ERROR MatrixProtocols().getMatrixProtocols():  " + ex);
        }finally{
            db.closeResultSet(protoNameRS);
            db.closeResultSet(rs);
            protoNameRS = null;
            rs = null;
        }
        
        return matrizProtocols;
    }
    
    //=========================================================================

    private String[][] printMatrix(String[][] matrizFinalProtocols) {
        String matrix[][] = new String[matrizFinalProtocols.length][3];

        int fila = 0;
        for (int i = matrizFinalProtocols.length - 1; i >= 0; i--) {
            matrix[fila][0] = matrizFinalProtocols[i][0];
            matrix[fila][1] = matrizFinalProtocols[i][1];
            matrix[fila][2] = matrizFinalProtocols[i][2];
            fila++;
        }
        return matrix;
    }



    //=========================================================================
}
