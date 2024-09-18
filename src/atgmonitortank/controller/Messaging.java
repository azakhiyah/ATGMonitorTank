/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package atgmonitortank.controller;

import java.io.IOException;
///import java.net.InetAddress;
import java.util.Map;
import java.util.logging.Logger;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

import atgmonitortank.utilities.Config;

/**
 *
 * @author zakhiyah arsal
 */
public class Messaging {
    static Logger mLog = Logger.getLogger(Messaging.class.getName());

    public static void sendData(int tankno,int consoleID,String SiteId, String ReadTime,String ProductID,float level,float volume,float density,float temperature,float mass,float volumeflowrate,float GSV, int Status) throws IOException {    
        Config properties = new Config();
        String broker = new String(properties.getProperty("mqqt.broker"));
        String topic = new String(properties.getProperty("mqqt.topic"));
        String user = new String(properties.getProperty("mqqt.user"));
        String passwrd = new String(properties.getProperty("mqtt.passwrd"));
        String clientId = "publisher";
        MemoryPersistence persistence = new MemoryPersistence();
        
        try {
              // Create connection to MQTT client
            MqttClient mqttClient = new MqttClient(broker, clientId, persistence);

            // Create connection option
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true);
            connectOptions.setUserName(user);
            connectOptions.setPassword(passwrd.toCharArray());

            // Connected to broker MQTT
            mqttClient.connect(connectOptions);

            // Data need to sent
            JSONObject data = new JSONObject();
            data.put("Level",level);
            data.put("Volume",volume);
            data.put("Density",density);
            data.put("Temperature",temperature);
            data.put("Mass",mass);
            data.put("VolumeFlowrate",volumeflowrate);
            data.put("GSV",GSV);
            
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("TankNo", tankno);
            jsonObject.put("ConsoleId", consoleID);
            jsonObject.put("SiteId", SiteId);
            jsonObject.put("ReadTime", ReadTime);
            jsonObject.put("ProductID", ProductID);
            jsonObject.put("Data", data);
            jsonObject.put("Status", Status);
            
            // Convert array string to JSON array
            String jsonString =  jsonObject.toString();

            // Create Message MQTT
            MqttMessage mqttMessage = new MqttMessage(jsonString.getBytes());
            //mqttMessage.setQos(qos);

            // Sent Message to topic MQTT
            mqttClient.publish(topic, mqttMessage);
            mLog.info("Send Data To HO");

            // Close Connection MQTT client
            mqttClient.disconnect();
            mqttClient.close();
            
        } catch (MqttException e) {
            e.printStackTrace();
            
        }
        
        
    }
    
    public static String buildJsonString(Map<String, Object> jsonObject) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            String key = entry.getKey();
            Object value = entry.getValue();
            sb.append("\"").append(key).append("\":");

            if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else if (value instanceof Boolean || value instanceof Number) {
                sb.append(value);
            } else {
                throw new IllegalArgumentException("Unsupported data type in JSON: " + value.getClass());
            }
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
    
     public static boolean CheckBrokerOnline() throws IOException {
        Config properties = new Config();
        String broker = new String(properties.getProperty("mqqt.broker"));
        String topic = new String(properties.getProperty("mqqt.topic"));
        String user = new String(properties.getProperty("mqqt.user"));
        String passwrd = new String(properties.getProperty("mqtt.passwrd"));
        String clientId = "publisher";
        Boolean statusBroker = false;
         try {
            MqttClient client = new MqttClient(broker, clientId);
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true);
            connectOptions.setUserName(user);
            connectOptions.setPassword(passwrd.toCharArray());

            mLog.info("Connecting to broker: " + broker);
            client.connect(connectOptions);
            mLog.info("Connected to broker: " + broker);

            // Check if the client is connected to the broker
            if (client.isConnected()) {
                //System.out.println("Broker is online.");
                statusBroker = true;
            } else {
               // System.out.println("Broker is offline.");
               statusBroker = false;
            }

            client.disconnect();
            client.close();
            //System.out.println("Disconnected from broker.");
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return statusBroker;
       
    }
    
    
    
}
