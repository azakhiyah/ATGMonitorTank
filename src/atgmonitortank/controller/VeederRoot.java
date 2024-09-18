/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.controller;

import com.fazecast.jSerialComm.SerialPort;
import com.fazecast.jSerialComm.SerialPortInvalidPortException;

import atgmonitortank.utilities.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;



/**
 *
 * @author zakhiyah arsal
 */
public class VeederRoot {

    //ATG GENERAL variabel
    public static String HEADER="";
    public static String TANKNO="";
    public static String CURRENT_DATETIME="";
    public static String CDT="";
    public static String TANKNO_DETAILS="";
    public static int TD_3=0;
    public static String PRODUCTCODE="";
    public static String FIELDCOUNT="";
    public static int FC_3=0;
    //INVENTORY variabel
    public static String STATUS="";
    public static String VOLUME="";
    public static Float V_3;
    public static String TCVOLUME="";
    public static Float TCV_3;
    public static String ULLAGE="";
    public static Float U_3;
    public static String HEIGHT="";
    public static Float H_3;
    public static String WATER="";
    public static Float W_3;
    public static String TEMPERATURE="";
    public static Float T_3;
    public static String WATERVOLUME="";
    public static Float WV_3;
    
    static Logger mLog = Logger.getLogger(VeederRoot.class.getName());

     public static void getData() throws IOException, ParseException, InterruptedException{
         
         SerialPort serialport = null;
         Config properties = new Config();
         int consoleid = Integer.parseInt(properties.getProperty("VeederRoot.console.id"));
         String port=new String(properties.getProperty("VeederRoot.serial.port"));
         int baudrate = Integer.parseInt(properties.getProperty("VeederRoot.serial.baudrate"));
         int databits = Integer.parseInt(properties.getProperty("VeederRoot.serial.baudrate"));
         int stopbits = Integer.parseInt(properties.getProperty("VeederRoot.serial.stopbits"));
         int parity = Integer .parseInt(properties.getProperty("VeederRoot.serial.parity"));
         int numPoints = Integer.parseInt(properties.getProperty("VeederRoot.tank.total")); 
         List<Integer> TankList = MariaDBSQLConnection.getTankList(consoleid);
         List<String> ProductID  = MariaDBSQLConnection.getProductId(consoleid);
         List<String> SiteID = MariaDBSQLConnection.getSiteID(consoleid);
            try {
                  
                  serialport = SerialPort.getCommPort(port);
                  serialport.openPort();
                  serialport.setComPortParameters(baudrate, databits, stopbits, parity);
                  int[] inventoryT1 = {0x01,0x69,0x32,0x30,0x31,0x30,0x3};
                  int lastIndex = inventoryT1.length - 1;
                  String lastIndexVal = Integer.toString(inventoryT1[lastIndex]);
                  
                         
                  
                  for (int i = 0; i < numPoints; i++) {
                      
                         SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
                         Date date = new Date();
                         String ReadTime = sf.format(date);
                
                        int b = i+1;
                        int k = TankList.get(i);
                        String Proid = ProductID.get(i);
                        String siteid = SiteID.get(i);
                        String lastIndexNew = lastIndexVal+b;
                        inventoryT1[lastIndex] = Integer.parseInt(lastIndexNew,16);
                        byte[] bytestosend = new byte[inventoryT1.length];
                        for (int j = 0; j < inventoryT1.length; j++) {
                                      bytestosend[j] = (byte) inventoryT1[j];
                             }  
                     
                        serialport.writeBytes(bytestosend, bytestosend.length);
                        Thread.sleep(500);
                        byte[] receivedData = new byte[serialport.bytesAvailable()];
                        int numBytes = serialport.readBytes(receivedData, receivedData.length);
                        String receivedString = new String(receivedData);
                        parseInventory2(receivedString);
                        
                        Boolean statusBroker = false;
                            try {
                                statusBroker=Messaging.CheckBrokerOnline();
                            } catch (Exception e) {
                                statusBroker=false;
                            }
                     mLog.info("Broker MQTT status is ("+statusBroker+").");
                 
                        if (!statusBroker) {
                               MariaDBSQLConnection.processedTankData(k, consoleid, siteid,Proid ,H_3, V_3, 0, T_3, 0, 0, 0, 0,10);
                               MariaDBSQLConnection.insertTankDataHistory(k, consoleid, siteid,Proid,H_3, V_3, 0, T_3, 0,0,0,0, 10);
                           } else {

                           Messaging.sendData(k, consoleid, siteid,ReadTime,Proid,H_3, V_3, 0, T_3, 0, 0, 0,20);
                           MariaDBSQLConnection.processedTankData(k, consoleid, siteid,Proid ,H_3, V_3, 0, T_3, 0, 0, 0, 0,20);
                           MariaDBSQLConnection.insertTankDataHistory(k, consoleid, siteid,Proid,H_3, V_3, 0, T_3, 0,0,0,0,20);
                           MariaDBSQLConnection.processedPendingData();
                        }

                        
                        
                        Thread.sleep(4000);

                  }
 
                  
                  serialport.closePort();//Close serial port
            
            } catch(SerialPortInvalidPortException ex) {
              
            }
         
         
     }
    
     public static void parseInventory (String acii) throws ParseException {
        
                  String tankNo = acii.substring(17, 19);
                  TD_3=Integer.parseInt(tankNo);
                  
                  String dateTime = acii.substring(7, 17);
                  Date date1=new SimpleDateFormat("yymmddhhmm").parse(dateTime);
                  CDT = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(date1);
                  
                  //String productCode = acii.substring(20,21);
                  //String status = acii.substring(21, 25);
                  
                  String volume = acii.substring(26, 34);
                  Long V_2=Long.parseLong(volume,16);
                  V_3=Float.intBitsToFloat(V_2.intValue());
                  
                  String tcVolume = acii.substring(34, 42);
                  Long TCV_2=Long.parseLong(tcVolume,16);
                  TCV_3=Float.intBitsToFloat(TCV_2.intValue());
                  
                  String ullage = acii.substring(42, 50);
                  Long U_2=Long.parseLong(ullage,16);
                  U_3=Float.intBitsToFloat(U_2.intValue())
                          ;
                  String height = acii.substring(50, 58);
                  Long H_2=Long.parseLong(height,16);
                  H_3=Float.intBitsToFloat(H_2.intValue());
                  
                  String water = acii.substring(58, 66);
                  Long W_2=Long.parseLong(water,16);
                  W_3=Float.intBitsToFloat(W_2.intValue());
                  
                  String temperature = acii.substring(66, 74);
                  Long T_2=Long.parseLong(temperature,16);
                  T_3=Float.intBitsToFloat(T_2.intValue());
                  
                  String waterVolume = acii.substring(74, 82);
                  Long WV_2=Long.parseLong(waterVolume,16);
                  WV_3=Float.intBitsToFloat(WV_2.intValue());
                  
                     
    } 
     
    public static void parseInventory2 (String acii) throws ParseException {
        
                  String tankNo = acii.substring(17, 19);
                  TD_3=Integer.parseInt(tankNo);
                  
                  String dateTime = acii.substring(7, 17);
                  Date date1=new SimpleDateFormat("yymmddhhmm").parse(dateTime);
                  CDT = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(date1);
                  
                  String productCode = acii.substring(20,21);
                  String status = acii.substring(21, 25);
                  
                  String volume = acii.substring(26, 34);
                  Long V_2=Long.parseLong(volume,16);
                  V_3=Float.intBitsToFloat(V_2.intValue());
                  
                  String tcVolume = acii.substring(34, 42);
                  Long TCV_2=Long.parseLong(tcVolume,16);
                  TCV_3=Float.intBitsToFloat(TCV_2.intValue());
                  
                  String ullage = acii.substring(42, 50);
                  Long U_2=Long.parseLong(ullage,16);
                  U_3=Float.intBitsToFloat(U_2.intValue())
                          ;
                  String height = acii.substring(50, 58);
                  Long H_2=Long.parseLong(height,16);
                  H_3=Float.intBitsToFloat(H_2.intValue());
                  
                  String water = acii.substring(58, 66);
                  Long W_2=Long.parseLong(water,16);
                  W_3=Float.intBitsToFloat(W_2.intValue());
                  
                  String temperature = acii.substring(66, 74);
                  Long T_2=Long.parseLong(temperature,16);
                  T_3=Float.intBitsToFloat(T_2.intValue());
                  
                  String waterVolume = acii.substring(74, 82);
                  Long WV_2=Long.parseLong(waterVolume,16);
                  WV_3=Float.intBitsToFloat(WV_2.intValue());
                  
                 
                  mLog.info("Tank No : "+tankNo);
                  mLog.info("Date Time : "+CDT);
                  mLog.info("Product Code : "+productCode);
                  mLog.info("Status : "+status);
                  mLog.info("*******************************");
                  mLog.info("Volume : "+V_3);
                  mLog.info("Volume Hex : "+volume);
                  mLog.info("*******************************");
                  mLog.info("TC Volume : "+TCV_3);
                  mLog.info("TC Volume Hex : "+tcVolume);
                  mLog.info("*******************************");
                  mLog.info("Ullage : "+U_3);
                  mLog.info("Ullage Hex : "+ullage);
                  mLog.info("*******************************");
                  mLog.info("Height : "+H_3);
                  mLog.info("Height Hex: "+height);
                  mLog.info("*******************************");
                  mLog.info("Water : "+W_3);
                  mLog.info("Water Hex: "+water);
                  mLog.info("*******************************");
                  mLog.info("Temperature : "+T_3);
                  mLog.info("Temperature Hex : "+temperature);
                  mLog.info("*******************************");
                  mLog.info("Water Volume : "+WV_3);
                  mLog.info("Water Volume Hex : "+waterVolume);
                  mLog.info("*******************************");
                  mLog.info("*******************************");
        
    } 
    
    public static byte[] hexStringToByteArray(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
    
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
    
     public static void printBytesAsBinary(byte[] bytes) {
        for (byte b : bytes) {
            String binaryString = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            System.out.println(binaryString);
        }
    }
     
    public static void printBytesAsDecimal(byte[] bytes) {
        for (byte b : bytes) {
            System.out.println(Byte.toUnsignedInt(b));
        }
    }

     
}
