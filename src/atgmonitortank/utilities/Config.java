/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author zakhiyah arsal
 */
public class Config {
     Properties configFile;
   
   public Config() throws FileNotFoundException, IOException {  
        configFile = new Properties();
        File file = new File("./config.properties");
        FileInputStream fis = new FileInputStream(file);
        configFile.load(fis);
        fis.close();
   }
 
   public String getProperty(String key) {
      String value = this.configFile.getProperty(key);
      //int value = this.configFile.get(key);
      return value;
   }
    
}
