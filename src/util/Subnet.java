/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import org.apache.log4j.Logger;

/**
 *
 * @author puebla
 */
public class Subnet
{
    static final Logger logger = Logger.getLogger(Subnet.class);

    public Subnet()
    {
        class_subnets = new HashMap();
    }

    //******************************************************************************

    public void setSubnetMask(String ip)
    {
        sub_mask = ip;
        subnet_addr = ANDing.and(ip_addr, sub_mask);
        broadcast_addr = ANDing.broadcast(subnet_addr, sub_mask);
        calculateBitInformation(sub_mask, getNetworkClass());
        String minimumHostAddress[] = subnet_addr.split("[.]");
        minimumHostAddress[3] = Integer.toString(Integer.parseInt(minimumHostAddress[3]) + 1);
        min_host_range = (new StringBuilder(String.valueOf(minimumHostAddress[0]))).append(".").append(minimumHostAddress[1]).append(".").append(minimumHostAddress[2]).append(".").append(minimumHostAddress[3]).toString();
        String maximumHostAddress[] = broadcast_addr.split("[.]");
        maximumHostAddress[3] = Integer.toString(Integer.parseInt(maximumHostAddress[3]) - 1);
        max_host_range = (new StringBuilder(String.valueOf(maximumHostAddress[0]))).append(".").append(maximumHostAddress[1]).append(".").append(maximumHostAddress[2]).append(".").append(maximumHostAddress[3]).toString();
    }

    //******************************************************************************

    public void calculateBitInformation(String sub_mask, char network_class)
    {
        char bin_sub_mask[][] = Conversion.ipToBin(sub_mask);
        int num_bits = 0;
        for(int i = 0; i < 4; i++)
        {
            for(int n = 0; n < 8; n++)
                if(bin_sub_mask[i][n] == '1')
                    num_bits++;

        }

        int subnet_bits = num_bits - ((Integer)class_subnets.get(Character.valueOf(network_class))).intValue();
        sub_bits = subnet_bits;
        masked_bits = num_bits;
        total_subnets = (int)Math.pow(2D, subnet_bits);
        int host_bits = (32 - num_bits);
        this.host_bits = host_bits;
        total_hosts = (int)Math.pow(2D, host_bits);
    }

    //******************************************************************************

    public void setSubnetBits(int subnetBits)
    {
        sub_bits = subnetBits;
        int bits = subnetBits + ((Integer)class_subnets.get(Character.valueOf(net_class))).intValue();
        int re = 32 - bits;
        int mb = 32;
        String strt = "00000000000000000000000000000000";
        char b[] = strt.toCharArray();
        for(int n = 0; n <= re; n++)
            mb--;

        for(int i = 0; i <= mb; i++)
            b[i] = '1';

        String s = new String(b);
        char ip[][] = {
            s.substring(0, 8).toCharArray(), s.substring(8, 16).toCharArray(), s.substring(16, 24).toCharArray(), s.substring(24, 32).toCharArray()
        };
        String mask = (new StringBuilder(String.valueOf(Integer.toString(Integer.parseInt(new String(ip[0]), 2))))).append(".").append(Integer.toString(Integer.parseInt(new String(ip[1]), 2))).append(".").append(Integer.toString(Integer.parseInt(new String(ip[2]), 2))).append(".").append(Integer.toString(Integer.parseInt(new String(ip[3]), 2))).toString();
        setSubnetMask(mask);
    }

//******************************************************************************

    public long inet_aton(String ip) {
        String[] octeto;
        String delimiter = "\\.";
        long ip_num = 0;
        int exp = 3;

        octeto = ip.split(delimiter);

        if (ip.length() < 17)//i.e. para ipV4
        {
            for (int i = 0; i < octeto.length; i++) {
                ip_num += (Integer.parseInt(octeto[i]) * Math.pow(256, exp));
                exp--;
            }
        } else//i.e. si es ip V6
        {
            ip_num = ip.hashCode();
        }
        return ip_num;
    }



//=========================================================================


    public String inet_ntoa(String ip) {
        String ipString = null;
        InetAddress address = null;
        try {
            address = InetAddress.getByName(ip);
            ipString = address.getHostAddress();
        } catch (UnknownHostException ex) {
            logger.error("ERROR Subnet().inet_ntoa():  " + ex);
        }


        return ipString;
    }

    //******************************************************************************
    @SuppressWarnings("unchecked")
    public void setIPAddress(String ip)
    {
        ip_addr = ip;
        ip_blocks = ip.split("[.]");
        int f = Integer.parseInt(ip_blocks[0]);
        if(f > 255)
        {
            System.err.print("Not a binary octet");
        } else
        {
            if(f <= 127)
                net_class = 'a';
            if(f <= 191 && f >= 128)
                net_class = 'b';
            if(f <= 223 && f >= 192)
                net_class = 'c';
            if(f <= 239 && f >= 224)
                net_class = 'd';
            if(f <= 255 && f >= 240)
                net_class = 'e';
        }
        class_subnets.put(Character.valueOf('a'), Integer.valueOf(8));
        class_subnets.put(Character.valueOf('b'), Integer.valueOf(16));
        class_subnets.put(Character.valueOf('c'), Integer.valueOf(24));
        class_subnets.put(Character.valueOf('d'), Integer.valueOf(3));
        class_subnets.put(Character.valueOf('e'), Integer.valueOf(4));
    }

    //******************************************************************************

    public void setTotalSubnets(int totalSubnets)
    {
        total_subnets = totalSubnets;
        int subnetBits = (int)(Math.log(totalSubnets) / Math.log(2D));
        setSubnetBits(subnetBits);
    }

    //******************************************************************************

    public void setTotalHosts(int totalHosts)
    {
        total_hosts = totalHosts;
        int hostBits = (int)(Math.log(totalHosts) / Math.log(2D));
        int subnetBits = 32 - (hostBits + ((Integer)class_subnets.get(Character.valueOf(net_class))).intValue());
        setSubnetBits(subnetBits);
    }

    //******************************************************************************

    public void setMaskedBits(int maskedBits)
    {
        masked_bits = maskedBits;
        int subnetBits = masked_bits - ((Integer)class_subnets.get(Character.valueOf(net_class))).intValue();
        setSubnetBits(subnetBits);
    }

    //******************************************************************************

    public String getSubnetAddress()
    {
        return subnet_addr;
    }

    //******************************************************************************

    public char getNetworkClass()
    {
        return net_class;
    }

    //******************************************************************************

    public String getBroadcastAddress()
    {
        return broadcast_addr;
    }

    //******************************************************************************

    public int getSubnetBits()
    {
        return sub_bits;
    }

    //******************************************************************************

    public int getTotalSubnets()
    {
        return total_subnets;
    }

    //******************************************************************************

    public int getUsableSubnets()
    {
        return total_subnets - 2;
    }

    //******************************************************************************

    public int getMaskedBits()
    {
        return masked_bits;
    }

    //******************************************************************************

    public int getTotalHosts()
    {
        return total_hosts;
    }

    //******************************************************************************

    public int getUsableHosts()
    {
        return total_hosts - 2;
    }

    //******************************************************************************

    public String getMinimumHostAddressRange()
    {
        return min_host_range;
    }

    //******************************************************************************

    public String getMaximumHostAddressRange()
    {
        return max_host_range;
    }

    //******************************************************************************

    public String getSubnetMask()
    {
        return sub_mask;
    }

    //******************************************************************************

    public int getHostBits()
    {
        return host_bits;
    }

    //******************************************************************************

    public String getIPAddress()
    {
        return ip_addr;
    }

    //******************************************************************************

    private String ip_blocks[];
    private char net_class;
    private String ip_addr;
    private String subnet_addr;
    private String broadcast_addr;
    private int host_bits;
    private String sub_mask;
    private int sub_bits;
    private int total_subnets;
    private int masked_bits;
    private int total_hosts;
    private String min_host_range;
    private String max_host_range;
    private HashMap class_subnets;
}
