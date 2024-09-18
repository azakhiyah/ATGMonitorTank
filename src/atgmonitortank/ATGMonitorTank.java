/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;

import atgmonitortank.utilities.Config;


/**
 *
 * @author zakhiyah arsal
 */
public class ATGMonitorTank {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, ParseException, InterruptedException, IllegalArgumentException, InvocationTargetException {
        Config properties = new Config();
        String ATGList=new String(properties.getProperty("ATG"));
        String[] ATGList2 = ATGList.split(",");
        
        try {
            for (String className : ATGList2) {
                Class<?> clazz = Class.forName("atgmonitortank.controller."+className);
                Object instance = clazz.getDeclaredConstructor().newInstance();

                clazz.getMethod("getData").invoke(instance);
            }
} catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
            InstantiationException | InvocationTargetException e) {
    e.printStackTrace();
}

    }
    
}
