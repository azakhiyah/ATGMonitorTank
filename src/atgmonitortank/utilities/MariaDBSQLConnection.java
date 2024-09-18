/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.utilities;


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import atgmonitortank.controller.Messaging;

/**
 *
 * @author zakhiyah arsal
 */
public class MariaDBSQLConnection {
    
    public static java.sql.Date getCurrentDatetime() {
        java.util.Date today = new java.util.Date();
        return new java.sql.Date(today.getTime());
    }
    
    static Logger mLog = Logger.getLogger(MariaDBSQLConnection.class.getName());
    
    public static Connection getConnection() throws IOException, SQLException {
                Config properties = new Config();
                String driver=new String(properties.getProperty("db.driver"));
                String server_ip=new String(properties.getProperty("db.ip"));
                String port=new String(properties.getProperty("db.port"));
                String dbname=new String(properties.getProperty("db.name"));
                String username=new String(properties.getProperty("db.username"));
                String password=new String(properties.getProperty("db.password"));
                
		Connection conn = null;
		try {
                        Class.forName("org.mariadb.jdbc.Driver");
			StringBuilder url = new StringBuilder("jdbc:mariadb://");
                        url.append(server_ip).append(":").append(port).append("/").append(dbname);
                        //System.out.println(url.toString());
                        conn = DriverManager.getConnection(url.toString(), username, password);
                        //System.out.println("mariaDB Connection Created");
		} catch (ClassNotFoundException e) {	
			e.printStackTrace();
		}
	return conn;
	}
    
   
    
    /*
    Process Insert data :
    1.Select tank no berdasarkan consoleid lalu simpan kedalam array tank no
    2.looping bedasarkan nilai dalam array tank no untuk process :
      -insert data jika tank no belum available ditankdata
      -update data jika tank no available ditankdata
      -insert data kedalam tankhistiry
    */
    
    public static void insertTankData(int TankNo, int ConsoleId, String SiteID, String ProductID ,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV, int status) {
        SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date date = new Date();
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Insert data into tankdata");

            ps3 = conn.prepareStatement("Insert into tankdata " +
                "(TankNo,ConsoleID,SiteID,ReadTime,ProductID,Level,Volume,Density,Temperature,Mass,VolumeFlowrate,MassFlowrate,GSV,status) Values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Statement.RETURN_GENERATED_KEYS);

            ps3.setInt(1,TankNo); 
            ps3.setInt(2,ConsoleId);
            ps3.setString(3,SiteID);
            ps3.setString(4,sf.format(date));
            ps3.setString(5,ProductID); 
            ps3.setFloat(6,Level); 
            ps3.setFloat(7,Volume);
            ps3.setFloat(8,Density); 
            ps3.setFloat(9,Temperature); 
            ps3.setFloat(10,Mass); 
            ps3.setFloat(11,VolumeFlowrate);  
            ps3.setFloat(12,MassFlowrate); 
            ps3.setFloat(13, GSV);
            ps3.setInt(14,status); 
            
            ps3.executeUpdate();
            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void updateTankData(int TankNo, int ConsoleId, String SiteID, String ProductID ,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV,int status) {
        SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date date = new Date();
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Update data into tankdata");

            ps3 = conn.prepareStatement("UPDATE TankData SET "+
                    "ReadTime=?,Level=?,Volume=?,Density=?,Temperature=?,Mass=?,VolumeFlowrate=?,MassFlowrate=?,GSV=?,status=? "+
                    "where TankNo=? and ConsoleId=? and SiteID=? and ProductID=?");
            
            ps3.setString(1,sf.format(date));
            ps3.setFloat(2,Level); 
            ps3.setFloat(3,Volume);
            ps3.setFloat(4,Density); 
            ps3.setFloat(5,Temperature); 
            ps3.setFloat(6,Mass); 
            ps3.setFloat(7,VolumeFlowrate);  
            ps3.setFloat(8,MassFlowrate); 
            ps3.setFloat(9, GSV);
            ps3.setInt(10, status);
            ps3.setInt(11,TankNo); 
            ps3.setInt(12,ConsoleId);
            ps3.setString(13,SiteID);
            ps3.setString(14,ProductID);
                  
            ps3.executeUpdate();
            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void processedTankData(int TankNo, int ConsoleId, String SiteID, String ProductID ,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV, int status) {
        //SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        //Date date = new Date();
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Process insert data");

              ps3 = conn.prepareStatement(" select * from TankData where TankNo=? and ConsoleId=? and SiteID=?");
            
      
            ps3.setInt(1,TankNo); 
            ps3.setInt(2,ConsoleId);
            ps3.setString(3,SiteID);
                  
           ResultSet rs = ps3.executeQuery();
           if (rs.next()) {
               updateTankData(TankNo, ConsoleId, SiteID,ProductID ,Level, Volume, Density, Temperature, Mass, VolumeFlowrate, MassFlowrate, GSV,status);
            } else {
               insertTankData(TankNo, ConsoleId, SiteID,ProductID ,Level, Volume, Density, Temperature, Mass, VolumeFlowrate, MassFlowrate, GSV,status);
               }

            ps3.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static void insertTankDataHistory (int TankNo, int ConsoleId, String SiteID ,String ProductID,float Level, float Volume,float Density,float Temperature,float Mass,float VolumeFlowrate,float MassFlowrate,float GSV,int status) throws IOException {
        SimpleDateFormat sf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date date = new Date();
        UUID uuid = UUID.randomUUID();
        String uuidAsString = uuid.toString();
        
        Connection conn = null;
        PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            mLog.info("Insert data into tankdatahistory");

            ps3 = conn.prepareStatement("Insert into tankdatahistory " +
                "(RowID,TankNo,ConsoleID,SiteID,ReadTime,ProductID,Level,Volume,Density,Temperature,Mass,VolumeFlowrate,MassFlowrate,GSV,status) Values " +
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Statement.RETURN_GENERATED_KEYS);
            
            ps3.setString(1,uuidAsString); 
            ps3.setInt(2,TankNo); 
            ps3.setInt(3,ConsoleId);
            ps3.setString(4,SiteID);
            ps3.setString(5,sf.format(date)); 
            ps3.setString(6,ProductID);
            ps3.setFloat(7,Level); 
            ps3.setFloat(8,Volume);
            ps3.setFloat(9,Density); 
            ps3.setFloat(10,Temperature); 
            ps3.setFloat(11,Mass); 
            ps3.setFloat(12,VolumeFlowrate);  
            ps3.setFloat(13,MassFlowrate); 
            ps3.setFloat(14, GSV);
            ps3.setInt(15, status);
            ps3.executeUpdate();
            ps3.close();
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        
    }
    
    public static List<Integer> getTankList(int ConsoleID) {
     
        Connection conn = null;
        List<Integer> TankList = new ArrayList<Integer>();
        try {
            
            conn = getConnection();
            StringBuilder sqlgetTankList = new StringBuilder();
            sqlgetTankList.append ("select TankNo from Tanks");
            sqlgetTankList.append (" where ConsoleID = ").append(ConsoleID);
            sqlgetTankList.append(" and Enabled=1 order by tankNoATG asc;");
                    
            PreparedStatement stmtgetTankList = conn.prepareStatement(sqlgetTankList.toString());
            ResultSet rsgetTankList =  stmtgetTankList.executeQuery();
            ResultSetMetaData rsmd = rsgetTankList.getMetaData();
            

            
            while (rsgetTankList.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    TankList.add(rsgetTankList.getInt(i));
                }
            }

           conn.close();    
            
        } catch (Exception e) {
                e.printStackTrace();
        }
       return TankList;    
    }
    
    public static List<Integer> getTankNoATGList(int ConsoleID) {
     
        Connection conn = null;
        List<Integer> TankNoATGList = new ArrayList<Integer>();
        try {
            
            conn = getConnection();
            StringBuilder sqlgetTankNoATGList = new StringBuilder();
            sqlgetTankNoATGList.append ("select TankNoATG from Tanks");
            sqlgetTankNoATGList.append (" where ConsoleID = ").append(ConsoleID);
            sqlgetTankNoATGList.append(" and Enabled=1 order by tankNoATG asc;");
                    
            PreparedStatement stmtgetTankNoATGList = conn.prepareStatement(sqlgetTankNoATGList.toString());
            ResultSet rsgetTankNoATGList =  stmtgetTankNoATGList.executeQuery();
            ResultSetMetaData rsmd = rsgetTankNoATGList.getMetaData();
            

            
            while (rsgetTankNoATGList.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    TankNoATGList.add(rsgetTankNoATGList.getInt(i));
                }
            }

           conn.close();    
            
        } catch (Exception e) {
                e.printStackTrace();
        }
       return TankNoATGList;    
    }
    public static List<String> getProductId(int ConsoleID) {
     
        Connection conn = null;
        List<String> ProductId = new ArrayList<String>();
        try {
            
            conn = getConnection();
            StringBuilder sqlgetProductId = new StringBuilder();
            sqlgetProductId.append ("select ProductID from Tanks");
            sqlgetProductId.append (" where ConsoleID = ").append(ConsoleID);
            sqlgetProductId.append(" order by tankNoATG asc;");
                    
            PreparedStatement stmtgetProductId = conn.prepareStatement(sqlgetProductId.toString());
            ResultSet rsgetProductId =  stmtgetProductId.executeQuery();
            ResultSetMetaData rsmd = rsgetProductId.getMetaData();
            

            
            while (rsgetProductId.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    ProductId.add(rsgetProductId.getString(i));
                }
            }

           conn.close();    
            
        } catch (Exception e) {
                e.printStackTrace();
        }
       return ProductId;    
    }
    
    public static List<String> getSiteID(int ConsoleID) {
     
        Connection conn = null;
        List<String> SiteId = new ArrayList<String>();
        try {
            
            conn = getConnection();
            StringBuilder sqlgetSiteId = new StringBuilder();
            sqlgetSiteId.append ("select SiteID from Tanks");
            sqlgetSiteId.append (" where ConsoleID = ").append(ConsoleID);
            sqlgetSiteId.append(" order by tankNoATG asc;");
                    
            PreparedStatement stmtgetSiteId = conn.prepareStatement(sqlgetSiteId.toString());
            ResultSet rsgetSiteId =  stmtgetSiteId.executeQuery();
            ResultSetMetaData rsmd = rsgetSiteId.getMetaData();
            

            
            while (rsgetSiteId.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    SiteId.add(rsgetSiteId.getString(i));
                }
            }

           conn.close();    
            
        } catch (Exception e) {
                e.printStackTrace();
        }
       return SiteId;    
    }
    
    public static void  processedPendingData () {
        Connection conn = null;
        //PreparedStatement ps3 = null;
        
        try {
            conn = getConnection();
            StringBuilder sqlgetPendingData = new StringBuilder("select * from tankdatahistory where status = 10");
            PreparedStatement stmtgetPendingData = conn.prepareStatement(sqlgetPendingData.toString());
            ResultSet rsgetPendingData =  stmtgetPendingData.executeQuery();
            //ResultSetMetaData rsmd = rsgetPendingData.getMetaData();
             while (rsgetPendingData.next()) {
                 //Messaging.sendData(k, consoleid, siteid,ReadTime, Proid,Level, float_volume, Density, Temperature,Mass, VolumeFlowrate, float_GSV);
                String RowID = rsgetPendingData.getString("RowID");
                int TankNo = rsgetPendingData.getInt("TankNo");
                int ConsoleId = rsgetPendingData.getInt("ConsoleId");
                String SiteID = rsgetPendingData.getString("SiteID");
                String ReadTime = rsgetPendingData.getString("ReadTime");
                String ProductID = rsgetPendingData.getString("ProductID");
                Float Level = rsgetPendingData.getFloat("Level");
                Float Volume = rsgetPendingData.getFloat("Volume");
                Float Density = rsgetPendingData.getFloat("Density");
                Float Temperature = rsgetPendingData.getFloat("Temperature");
                Float Mass = rsgetPendingData.getFloat("Mass");
                Float VolumeFlowrate = rsgetPendingData.getFloat("VolumeFlowrate");
                //Float MassFlowrate = rsgetPendingData.getFloat("MassFlowrate");
                Float GSV = rsgetPendingData.getFloat("GSV");
                int status = rsgetPendingData.getInt("status");
                mLog.info("Sent Data Pending from TankDataHistory");
                Messaging.sendData(TankNo, ConsoleId, SiteID,ReadTime, ProductID,Level, Volume, Density, Temperature,Mass, VolumeFlowrate, GSV,status);
                StringBuilder sqlUpdateProccode = new StringBuilder();
                sqlUpdateProccode.append("UPDATE tankdatahistory SET status = 20 ");
                sqlUpdateProccode.append(" WHERE RowID = '").append(RowID).append("'");
                PreparedStatement stmtUpdateProccode = conn.prepareStatement(sqlUpdateProccode.toString());
                stmtUpdateProccode.execute();
             } 
                conn.close();    
            } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
}
