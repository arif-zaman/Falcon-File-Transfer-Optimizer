package client.utils;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

public class HostResolution extends Thread {
    String hostname;
    InetAddress[] allIPs;

    public HostResolution(String hostname) {
        this.hostname = hostname;
    }

    @Override
    public void run() {
        try {
            if (hostname.contains("ie")) {
                allIPs = new InetAddress[24];
                List<InetAddress> inetAddressList = new LinkedList<>();
                for (int i = 3; i < 28; i++) {
                    if (i == 25)
                        continue;
                    inetAddressList.add(InetAddress.getByName("ie" + String.format("%02d", i) + ".ncsa.illinois.edu"));
                }
                allIPs = inetAddressList.toArray(allIPs);
                for (int i = 0; i < allIPs.length; i++) {
                    System.out.println(allIPs[i]);
                }
                return;
            }
            System.out.println("Getting IPs for:" + hostname);
            allIPs = InetAddress.getAllByName(hostname);
            System.out.println("Found IPs for:" + hostname);
            //System.out.println("IPs for:" + du.getHost());
            for (int i = 0; i < allIPs.length; i++) {
                System.out.println(allIPs[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public InetAddress[] getAllIPs() {
        return allIPs;
    }
}