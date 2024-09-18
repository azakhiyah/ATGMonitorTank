/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.controller;

import com.ghgande.j2mod.modbus.facade.ModbusTCPMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;

import atgmonitortank.utilities.Config;
import atgmonitortank.utilities.MariaDBSQLConnection;
import atgmonitortank.utilities.StringOperation;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author zakhiyah arsal
 */
public class EndressHauser {
    static Logger mLog = Logger.getLogger(EndressHauser.class.getName());
     public static void getData() throws IOException {
        ModbusTCPMaster master;
        Config properties = new Config();
        int consoleid = Integer.parseInt(properties.getProperty("EndressHauser.console.id"));
        String ip = new String(properties.getProperty("EndressHauser.ip"));
        int port = Integer.parseInt(properties.getProperty("EndressHauser.port"));
        int numPoints = Integer.parseInt(properties.getProperty("EndressHauser.tank.total"));
        int length_modbusreg = Integer.parseInt(properties.getProperty("EndressHauser.modbusreg.length"));
        int slaveId = Integer.parseInt(properties.getProperty("EndressHauser.slave.id"));
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
                String Proid = ProductID.get(i);
                String siteid = SiteID.get(i);
                int TankNoATG = TankNoATGList.get(i);

                int difference = TankNo - TankNoATG;
                if (difference == 0) {
                    start_reg = (TankNoATG - 1)*length_modbusreg;
                } else {
                    start_reg = (TankNoATG + difference - 1) * length_modbusreg;
                }
                InputRegister[] data = master.readInputRegisters(slaveId, start_reg, 125);
                mLog.info("Request Data ATG EndressHauser Tank No : "+TankNo);
                //level
                String llevel  = Integer.toHexString(data[0].getValue());
                String rlevel  = Integer.toHexString(data[1].getValue());
                String level = StringOperation.InsertString(rlevel) + StringOperation.InsertString(llevel);
                Long long_level=Long.parseLong(level,16);
                float float_level=Float.intBitsToFloat(long_level.intValue());
                mLog.info("level Float "+TankNoATG+"= "+float_level);
                //volume
                String lvolume  = Integer.toHexString(data[4].getValue());
                String rvolume  = Integer.toHexString(data[5].getValue());
                String volume = StringOperation.InsertString(rvolume) + StringOperation.InsertString(lvolume);
                Long long_volume=Long.parseLong(volume,16);
                float float_volume=Float.intBitsToFloat(long_volume.intValue());
                mLog.info("volume Float "+TankNoATG+"= "+float_volume);
                //density
                String ldensity  = Integer.toHexString(data[36].getValue());
                String rdensity  = Integer.toHexString(data[37].getValue());
                String density = StringOperation.InsertString(rdensity) + StringOperation.InsertString(ldensity);
                Long long_density=Long.parseLong(density,16);
                float float_density=Float.intBitsToFloat(long_density.intValue());
                mLog.info("density Float "+TankNoATG+"= "+float_density);
                
                //temperature
                String ltemperature  = Integer.toHexString(data[20].getValue());
                String rtemperature  = Integer.toHexString(data[21].getValue());
                String temperature = StringOperation.InsertString(rtemperature) + StringOperation.InsertString(ltemperature);
                Long long_temperature=Long.parseLong(temperature,16);
                float float_temperature=Float.intBitsToFloat(long_temperature.intValue());
                mLog.info("temperature Float "+TankNoATG+"= "+float_temperature);
                
                //mass
                String lmass  = Integer.toHexString(data[56].getValue());
                String rmass  = Integer.toHexString(data[57].getValue());
                String mass = StringOperation.InsertString(rmass) + StringOperation.InsertString(lmass);
                Long long_mass=Long.parseLong(mass,16);
                float float_mass=Float.intBitsToFloat(long_mass.intValue());
                mLog.info("mass Float "+TankNoATG+"= "+float_mass);
             
                float VolumeFlowrate=0;
                
                //massflowrate
                String lmassflowrate  = Integer.toHexString(data[64].getValue());
                String rmassflowrate  = Integer.toHexString(data[65].getValue());
                String massflowrate = StringOperation.InsertString(rmassflowrate) + StringOperation.InsertString(lmassflowrate);
                Long long_massflowrate=Long.parseLong(massflowrate,16);
                float float_massflowrate=Float.intBitsToFloat(long_massflowrate.intValue());
                mLog.info("massflowrate Float "+TankNoATG+"= "+float_massflowrate);
                
                Boolean statusBroker = false;
                            try {
                                statusBroker=Messaging.CheckBrokerOnline();
                            } catch (Exception e) {
                                statusBroker=false;
                            }
                     mLog.info("Broker MQTT status is ("+statusBroker+").");
                 
                  if (!statusBroker) {
                         MariaDBSQLConnection.processedTankData(TankNo, consoleid, siteid,Proid, float_level, float_volume, float_density, float_temperature, float_mass,VolumeFlowrate,float_massflowrate,float_volume,10);
                         MariaDBSQLConnection.insertTankDataHistory(TankNo, consoleid, siteid,Proid,float_level, float_volume, float_density, float_temperature, float_mass,VolumeFlowrate,float_massflowrate,float_volume,10);
                     } else {
                     
                     Messaging.sendData(TankNo, consoleid, siteid,ReadTime,Proid,float_level, float_volume, float_density, float_temperature, float_mass, VolumeFlowrate, float_volume,20);
                     MariaDBSQLConnection.processedTankData(TankNo, consoleid, siteid,Proid, float_level, float_volume, float_density, float_temperature, float_mass,VolumeFlowrate,float_massflowrate,float_volume,20);
                     MariaDBSQLConnection.insertTankDataHistory(TankNo, consoleid, siteid,Proid,float_level, float_volume, float_density, float_temperature, float_mass,VolumeFlowrate,float_massflowrate,float_volume,20);
                     MariaDBSQLConnection.processedPendingData();
                  }
                
                Thread.sleep(4000);
            }

                        master.disconnect();
        } catch (Exception e) {
            mLog.info(e.getMessage());
        }
 
    }
    
}
