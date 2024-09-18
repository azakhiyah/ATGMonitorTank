/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.controller;

import com.ghgande.j2mod.modbus.Modbus;
import com.ghgande.j2mod.modbus.facade.ModbusSerialMaster;
import com.ghgande.j2mod.modbus.procimg.InputRegister;
import com.ghgande.j2mod.modbus.util.SerialParameters;

import atgmonitortank.utilities.*;

import java.io.IOException;
//import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 *
 * @author zakhiyah arsal
 */
public class Emerson {
    
    static Logger mLog = Logger.getLogger(Emerson.class.getName());
    
    public static void getData() throws IOException {
    //public static ModbusSerialMaster getData() throws IOException {
          Handler[] handlers = mLog.getParent().getHandlers();
            for (Handler handler : handlers) {
                if (handler instanceof ConsoleHandler) {
                    handler.setFormatter(new SimpleFormatter() {
                        public String format(LogRecord record) {
                            return record.getMessage() + System.lineSeparator();
                        }
                    });
                }
            }
         ModbusSerialMaster master;
        
         Config properties = new Config();
         int consoleid = Integer.parseInt(properties.getProperty("Emerson.console.id"));
         String port=new String(properties.getProperty("Emerson.serial.port"));
         int level_startaddress = Integer.parseInt(properties.getProperty("Emerson.level.address"))-1;
         int density_startaddress = Integer.parseInt(properties.getProperty("Emerson.density.address"))-1;
         int temperature_startaddress = Integer.parseInt(properties.getProperty("Emerson.temperature.address"))-1;
         int volume_startaddress = Integer.parseInt(properties.getProperty("Emerson.volume.address"))-1;
         int gsv_startaddress = Integer.parseInt(properties.getProperty("Emerson.gsv.address"))-1;
         //int volFlowrate_startaddress = Integer.parseInt(properties.getProperty("Emerson.volumeflowrate.address"))-1;
         int numPoints = Integer.parseInt(properties.getProperty("Emerson.tank.total")); 
         int numPoints2 = numPoints * 2; 
         int slaveId = Integer.parseInt(properties.getProperty("Emerson.slave.id")); 
         List<Integer> TankList = MariaDBSQLConnection.getTankList(consoleid);
         List<String> ProductID  = MariaDBSQLConnection.getProductId(consoleid);
         List<String> SiteID = MariaDBSQLConnection.getSiteID(consoleid);
         
         try {
             SerialParameters parameters = new SerialParameters();
             parameters.setPortName(port);
             parameters.setOpenDelay(1000);
             parameters.setEncoding(Modbus.SERIAL_ENCODING_RTU);
             master = new ModbusSerialMaster(parameters);
             master.connect();
             InputRegister[] level = master.readMultipleRegisters(slaveId, level_startaddress, numPoints);
             InputRegister[] density = master.readMultipleRegisters(slaveId, density_startaddress, numPoints);
             InputRegister[] temperature= master.readMultipleRegisters(slaveId, temperature_startaddress, numPoints);
             InputRegister[] volume= master.readMultipleRegisters(slaveId, volume_startaddress, numPoints2);
             InputRegister[] gsv= master.readMultipleRegisters(slaveId, gsv_startaddress, numPoints2);
             //InputRegister[] volumeflowrate= master.readMultipleRegisters(slaveId, volFlowrate_startaddress, numPoints2);
             int LevelInt,DensityInt,TemperatureInt;
             float Level,Density,Temperature;
             int count = 0;
             String rVol,lVol,rGSV,lGSV = null;
             //String rVolFlow,lVolFlow = null;
             int a,b;
             for (int i = 0; i < numPoints;i++) {
                   count++;
                   SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                   Date date = new Date();
                   String ReadTime = sf.format(date);
                   int k = TankList.get(i);
                   mLog.info("Request Data ATG Emerson Tank No : "+k);
                   String Proid = ProductID.get(i);
                   String siteid = SiteID.get(i);
                   
                   /*NEED To FIX kadang nilai 0 dibelakang hilang*/
                   LevelInt =level[i].getValue();
                   Float f = new Float(LevelInt);
                   //Level in Meter
                   //Level = f.floatValue()/1000;
                   Level = f.floatValue();
                   /*##########*/
                   
                   DensityInt =density[i].getValue();
                   Float d = new Float(DensityInt);
                   // Density in Kg/m3
                   Density = d.floatValue()/10;
                   //Density = d.floatValue()*100;
                   TemperatureInt =temperature[i].getValue();
                   Float t = new Float(TemperatureInt);
                   //Temperature in celcius
                   Temperature = t.floatValue() / 10;
                   
                   a = i + (count - 1); 
                   b= a+1;
                   
                 
                   lVol = Integer.toString(volume[a].getValue());
                   rVol = Integer.toString(volume[b].getValue());
                   String string_volume = rVol + lVol;
                   float float_volume = (Float.parseFloat(string_volume));
                   //BigDecimal bigDecimal_volume = new BigDecimal(string_volume);

                   

                   lGSV = Integer.toString(gsv[a].getValue());
                   rGSV = Integer.toString(gsv[b].getValue());
                   String string_GSV = rGSV + lGSV;
                   //BigDecimal bigDecimal_GSV = new BigDecimal(string_GSV);
                   //float float_GSV_no_scientific = bigDecimal_GSV.floatValue();
                   float float_GSV = (Float.parseFloat(string_GSV));
                   
                   
          
                   float VolumeFlowrate=0;
                   
                   float Mass=0;
                   float MassFlowrate=0;
                    
                   //mLog.info("Tab("+i+")Net connection status is ("+vNETOK+").");
                     mLog.info("Level T"+k+" : "+ Level +" detail ("+f+")");
                     mLog.info("Density T"+k+": " +  Density);
                     mLog.info("Temperature T"+k+": " + Temperature);
                     mLog.info("Volume T"+k+" : "+float_volume+" detail ("+rVol+"/"+volume[b].getValue()+"#"+lVol+"/"+volume[a].getValue()+")");
                     mLog.info("GSV T"+k+" : "+float_GSV+" detail ("+rGSV+"/"+gsv[b].getValue()+"#"+lGSV+"/"+gsv[a].getValue()+")");
                     mLog.info("VolumeFlowRate T"+k+" : "+VolumeFlowrate);
                     
                   Boolean statusBroker = false;
                            try {
                                statusBroker=Messaging.CheckBrokerOnline();
                            } catch (Exception e) {
                                statusBroker=false;
                            }
                     mLog.info("Broker MQTT status is ("+statusBroker+").");
                     
                     if (!statusBroker) {
                         MariaDBSQLConnection.processedTankData(k, consoleid, siteid,Proid ,Level, float_volume, Density, Temperature,Mass,VolumeFlowrate,MassFlowrate,float_GSV,10);
                         MariaDBSQLConnection.insertTankDataHistory(k, consoleid, siteid,Proid,Level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_GSV, 10);
        
                     } else {
                     
                     Messaging.sendData(k, consoleid, siteid,ReadTime, Proid,Level, float_volume, Density, Temperature,Mass, VolumeFlowrate, float_GSV,20);
                     MariaDBSQLConnection.processedTankData(k, consoleid, siteid,Proid ,Level, float_volume, Density, Temperature,Mass,VolumeFlowrate,MassFlowrate,float_GSV,20);
                     MariaDBSQLConnection.insertTankDataHistory(k, consoleid, siteid,Proid,Level, float_volume, Density, Temperature, Mass,VolumeFlowrate,MassFlowrate,float_GSV, 20);
                     MariaDBSQLConnection.processedPendingData();
                     }
                     
                     Thread.sleep(4000);
             }

// <editor-fold defaultstate="collapsed" desc="#how to read data">             
//            InputRegister[] level = master.readMultipleRegisters(slaveId, level_startaddress, numPoints);
//             for (int i = 0; i < level.length; i++) {
//                    //System.out.println("Level " + (level_startaddress + i) + ": " + level[i]);
//                     int n = i+1;
//                     //System.out.println("Level T"+n+" : "+Arrays.toString(level));
//                     System.out.println("Level T"+n+" : "+ level[i]);
//             }
//             
//             
//             InputRegister[] density = master.readMultipleRegisters(slaveId, density_startaddress, numPoints);
//             for (int i = 0; i < density.length; i++) {
//                    //System.out.println("Density " + (density_startaddress + i) + ": " + density[i]);
//                    
//                     int n = i+1;
//                     //System.out.println("Density T"+n+" : "+Arrays.toString(density));
//                     System.out.println("Density T"+n+": " +  density[i]);
//                }
//             
//             InputRegister[] temperature= master.readMultipleRegisters(slaveId, temperature_startaddress, numPoints);
//             for (int i = 0; i < temperature.length; i++) {
//                    //System.out.println("Temperature " + (temperature_startaddress + i) + ": " + temperature[i]);
//                    
//                     int n = i+1;
//                    //System.out.println("Temperature T"+n+" : "+Arrays.toString(temperature));
//                    System.out.println("Temperature T"+n+": " + temperature[i]);
//                }
//             
//             String rVol = null,lVol = null;
//             InputRegister[] volume= master.readMultipleRegisters(slaveId, volume_startaddress, numPoints2);
//             for (int i = 0; i < volume.length; i+=2) {
//                    lVol = Integer.toString(volume[i].getValue());
//                    rVol = Integer.toString(volume[i+1].getValue());
//                    //System.out.println("lValue:" + lVol);
//                    //System.out.println("rValue:" + rVol);
//                     int n = i+1;
//                    String fVolume = rVol + lVol;
//                    int Volume = Integer.parseInt(fVolume);
//                    //System.out.println("Volume T"+ i+1+" : "+Arrays.toString(volume));
//                    System.out.println("Volume T"+n+" : "+Volume);
//                    //System.out.println("volume :" + Volume);
//                    
//                }
//             
//             InputRegister[] gsv= master.readMultipleRegisters(slaveId, gsv_startaddress, numPoints2);
//             String rGSV = null,lGSV= null;
//             for (int i = 0; i < gsv.length; i+=2) {
//                    lGSV = Integer.toString(gsv[i].getValue());
//                    rGSV = Integer.toString(gsv[i+1].getValue());
//                    //System.out.println("lValue:" + lGSV);
//                    //System.out.println("rValue:" + rGSV);
//                    int n = i+1;
//                    String fGSV = rGSV + lGSV;
//                    int GSV = Integer.parseInt(fGSV);
//                   // System.out.println("GSV T"+ i+1+" : "+Arrays.toString(gsv));
//                    System.out.println("GSV T"+n+" : "+GSV);
//                   // System.out.println("GSV :"+GSV);
//
//                }
// </editor-fold>

             master.disconnect();
         } catch (Exception e) {
            mLog.info(e.getMessage());
         }
        
 
        
    }
    
    
}
