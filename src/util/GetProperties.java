package util;

import java.io.FileInputStream;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.util.Properties;

/**
 * This class reads any properties file.
 * @author skuarch
 */
public class GetProperties {

    static final Logger logger = Logger.getLogger(GetProperties.class);
    private String fileProperties;

    //==========================================================================
    /**
     * Construct overloaded with parameter.
     * @param nameFileProperties String.
     */
    public GetProperties(String nameFileProperties) {

        PropertyConfigurator.configure("configuration/log.properties");

        try {

            if (nameFileProperties == null) {
                return;
            }

            this.fileProperties = nameFileProperties;

        } catch (Exception e) {
            logger.error("ERROR: GetProperties().GetProperties()", e);
        } //end try-catch

    } //end GetProperties (construct overloaded)

    //==========================================================================
    /**
     * This method load returns a Prop to be used by getProperties.
     * @return value of prop
     */
    private Properties readFileProperties() {

        Properties props = null;
        FileInputStream fis = null;

        try {

            props = new Properties();
            fis = new FileInputStream(fileProperties);
            props.load(fis);

        } catch (Exception e) {
            logger.error("ERROR: GetProperties().readFileProperties()", e);
        } //end try-catch

        if (props == null) {
            logger.error("ERROR: GetProperties().getProperties() readFileProperties null");
        }

        return props;
    } //end ReadFileProperties

    //==========================================================================
    /**
     *  returns a string with the value of property.
     * @param namePropertie String
     * @return String with prop
     */
    public String getProperties(String namePropertie) {

        if (namePropertie.equals("")) {
            logger.error("ERROR: GetProperties().getProperties().namePropertie is empty");
            return "";
        }

        String value = null;

        try {

            value = readFileProperties().getProperty(namePropertie);

            if (value == null) {
                logger.error("ERROR: GetProperties().getProperties() returning null");
            }

        } catch (Exception e) {
            logger.error("ERROR: GetProperties().getProperties() ", e);
        } //end try-catch

        return value;
    } //end getProperties

    //==========================================================================
    public int getIntProperties(String namePropertie) {

        if (namePropertie.equals("")) {
            logger.error("ERROR: GetProperties().getIntProperties().namePropertie is empty");
        }

        String value = null;
        int num = 0;

        try {

            value = readFileProperties().getProperty(namePropertie);
            num = Integer.parseInt(value);

            if (value == null) {
                logger.error("ERROR: GetProperties().getIntProperties() returning null");
            }

        } catch (Exception e) {
            logger.error("ERROR: GetProperties().getIntProperties() ", e);
        } //end try-catch

        return num;
    } //end getProperties
} //end class

