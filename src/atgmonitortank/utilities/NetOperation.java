/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.utilities;

import java.net.InetAddress;

/**
 *
 * @author zakhiyah arsal
 */
public class NetOperation {
    
    public static boolean isHostnameLive(String MQTTServer) {
        try {
            InetAddress address = InetAddress.getByName(MQTTServer);
            return address.isReachable(5000); // Timeout after 5 seconds
        } catch (Exception e) {
            return false;
        }
    }
    
}
