/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

/**
 *
 * @author puebla
 */
public class ANDing
{

    public ANDing()
    {
    }

    public static String and(String ip1, String ip2)
    {
        char ip_bin1[][] = Conversion.ipToBin(ip1);
        char ip_bin2[][] = Conversion.ipToBin(ip2);
        char ip_result[][] = new char[4][8];
        for(int i = 0; i < 8; i++)
        {
            if(ip_bin1[0][i] == '0' || ip_bin2[0][i] == '0')
                ip_result[0][i] = '0';
            else
                ip_result[0][i] = '1';
            if(ip_bin1[1][i] == '0' || ip_bin2[1][i] == '0')
                ip_result[1][i] = '0';
            else
                ip_result[1][i] = '1';
            if(ip_bin1[2][i] == '0' || ip_bin2[2][i] == '0')
                ip_result[2][i] = '0';
            else
                ip_result[2][i] = '1';
            if(ip_bin1[3][i] == '0' || ip_bin2[3][i] == '0')
                ip_result[3][i] = '0';
            else
                ip_result[3][i] = '1';
        }

        return (new StringBuilder(String.valueOf(Conversion.toDecimal(new String(ip_result[0]))))).append(".").append(Conversion.toDecimal(new String(ip_result[1]))).append(".").append(Conversion.toDecimal(new String(ip_result[2]))).append(".").append(Conversion.toDecimal(new String(ip_result[3]))).toString();
    }

    public static String broadcast(String ip1, String ip2)
    {
        char ip_bin1[][] = Conversion.ipToBin(ip1);
        char ip_bin2[][] = Conversion.ipToBin(ip2);
        char ip_result[][] = new char[4][8];
        for(int i = 0; i < 8; i++)
        {
            if(ip_bin2[0][i] == '0')
                ip_result[0][i] = '1';
            else
                ip_result[0][i] = ip_bin1[0][i];
            if(ip_bin2[1][i] == '0')
                ip_result[1][i] = '1';
            else
                ip_result[1][i] = ip_bin1[1][i];
            if(ip_bin2[2][i] == '0')
                ip_result[2][i] = '1';
            else
                ip_result[2][i] = ip_bin1[2][i];
            if(ip_bin2[3][i] == '0')
                ip_result[3][i] = '1';
            else
                ip_result[3][i] = ip_bin1[3][i];
        }

        return (new StringBuilder(String.valueOf(Conversion.toDecimal(new String(ip_result[0]))))).append(".").append(Conversion.toDecimal(new String(ip_result[1]))).append(".").append(Conversion.toDecimal(new String(ip_result[2]))).append(".").append(Conversion.toDecimal(new String(ip_result[3]))).toString();
    }
}
