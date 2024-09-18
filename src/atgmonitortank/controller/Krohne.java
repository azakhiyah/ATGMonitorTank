package atgmonitortank.controller;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;

import atgmonitortank.utilities.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author zakhiyah arsal
 */
public class Krohne {
    static Logger mLog = Logger.getLogger(Krohne.class.getName()); 
    public static void getData() throws IOException {
        ModbusTCPMaster master;
        Config properties = new Config();
        int consoleid = Integer.parseInt(properties.getProperty("Krohne.console.id"));
        String ip = new String(properties.getProperty("Krohne.ip"));
        int port = Integer.parseInt(properties.getProperty("Krohne.port"));
        int numPoints = Integer.parseInt(properties.getProperty("Krohne.tank.total"));
        int length_modbusreg = Integer.parseInt(properties.getProperty("Krohne.modbusreg.length"));
        int slaveId = Integer.parseInt(properties.getProperty("Krohne.slave.id"));
        List<Integer> TankList = MariaDBSQLConnection.getTankList(consoleid);
        List<Integer> TankNoATGList = MariaDBSQLConnection.getTankNoATGList(consoleid);
        List<String> ProductID  = MariaDBSQLConnection.getProductId(consoleid);
        List<String> SiteID = MariaDBSQLConnection.getSiteID(consoleid);
        int start_reg = 0;
        
        try {
            master = new ModbusTCPMaster(ip, port, 10000, true);
            master.connect();
            for (int i = 0; i < numPoints;i++) {
                SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                Date date = new Date();
                String ReadTime = sf.format(date);
                int TankNo = TankList.get(i);
                mLog.info("Request Data ATG Krohne Tank No : "+TankNo);
                String Proid = ProductID.get(i);
                String siteid = SiteID.get(i);
                int TankNoATG = TankNoATGList.get(i);
                
                
                int difference = TankNo - TankNoATG;
                if (difference == 0) {
                    start_reg = (TankNoATG - 1)*length_modbusreg;
                } else {
                    start_reg = (TankNoATG + difference - 1) * length_modbusreg;
                }
                
                InputRegister[] data = master.readMultipleRegisters(slaveId, start_reg, length_modbusreg);
                //System.out.println("Data "+b+"= "+Arrays.toString(data));
                String llevel  = Integer.toHexString(data[0].getValue());
                String rlevel  = Integer.toHexString(data[1].getValue());
                String level = StringOperation.InsertString(rlevel) + StringOperation.InsertString(llevel);
                Long long_level=Long.parseLong(level,16);
                float float_level=Float.intBitsToFloat(long_level.intValue());
                mLog.info("level Float "+TankNoATG+"= "+float_level);
                
                String lvolume  = Integer.toHexString(data[2].getValue());
                String rvolume  = Integer.toHexString(data[3].getValue());
                String volume = StringOperation.InsertString(rvolume) + StringOperation.InsertString(lvolume);
                Long long_volume=Long.parseLong(volume,16);
                float float_volume=Float.intBitsToFloat(long_volume.intValue());
                mLog.info("volume Float "+TankNoATG+"= "+float_volume);
                
                float Density=0;
                float Temperature=0;
                float Mass=0;
                float VolumeFlowrate=0;
                float MassFlowrate=0;
                //float GSV=0;
                
                Boolean statusBroker = false;
                            try {
                                statusBroker=Messaging.CheckBrokerOnline();
                            } catch (Exception e) {
                                statusBroker=false;
                            }
                 mLog.info("Broker MQTT status is ("+statusBroker+").");
                 
                  if (!statusBroker) {
                         MariaDBSQLConnection.processedTankData(TankNo, consoleid, siteid,Proid, float_level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_volume,10);
                         MariaDBSQLConnection.insertTankDataHistory(TankNo, consoleid, siteid,Proid,float_level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_volume,10);
                     } else {
                     
                      Messaging.sendData(TankNo, consoleid, siteid,ReadTime,Proid,float_level, float_volume,Density,Temperature, Mass, VolumeFlowrate, float_volume,20);
                      MariaDBSQLConnection.processedTankData(TankNo, consoleid, siteid,Proid, float_level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_volume,20);
                      MariaDBSQLConnection.insertTankDataHistory(TankNo, consoleid, siteid,Proid,float_level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_volume, 20);
                      MariaDBSQLConnection.processedPendingData();
                  }

                Thread.sleep(4000);
            }
            master.disconnect();
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
                
    }
    
    
    
}
