/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.reltio.QuickTools;

/**
 *
 * @author admin
 */

import com.datastax.driver.core.utils.Bytes;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;


public class Cust405Segmentation {
    private static String ent_hist_source_path;
 
	public static void main(String[] args) throws IOException {
         try {
            ent_hist_source_path = args[0];
        } catch (Exception a0) {
            System.err.println("Please Specify the Entity_hist_Source path csv file as First Argument");
            System.exit(0);
        }
		//PrintWriter  writer = new PrintWriter(new BufferedWriter(new FileWriter("history_output.json")));
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(ent_hist_source_path), "UTF-8"));
		
               
		char buf[] = {'\011'}; // Tab char
		String tab = new String(buf);
		
		String line = null;
                
                Gson gson = new Gson();
                int count = 0;
                int recordsInFile = 0;
                Date date = new Date();
               DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
               String dt=dateFormat.format(date).substring(0,11).replaceAll("/", "");
               String outputFileName="Report"+dt+".csv";
        try (BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(outputFileName))) {
            System.out.println(dateFormat.format(date));
            for  (;(line = reader.readLine()) != null; ) {
                //System.out.println(line);
                date = new Date();
                StringTokenizer st = new StringTokenizer(line, tab);
                String casendraId = st.nextToken(); //
                String timestamp = st.nextToken();
                String history = st.nextToken();
                history = history.substring(1, history.length() -1); // remove double quotes from beginning and end of the string
                

                
                String historyJson = Decompressor.decompress(Bytes.fromHexString(history).array());
                
                JsonObject jsonObject = gson.fromJson(historyJson, JsonObject.class);
                if (jsonObject != null && !jsonObject.isJsonNull())
                {
                    String custId=getCustomerID(jsonObject);
                    String updatedTime=getRecordUpdateDate(jsonObject);
                    String updatedBy=getRecordChangedByUserName(jsonObject);
                    String uri=getUri(jsonObject);
                    String type=getType(jsonObject);
                    outputFileWriter.write(custId+","+updatedTime+","+updatedBy+","+uri+","+type+"\n");
                    recordsInFile++;
                    /*
                    if(updatedBy.contains("Automatic merge process at 2014-09-17")){
                    System.out.println(historyJson);
                    break;
                    }
                    */
                    
                }
                else
                {
                    //System.out.println(line);
                    //System.out.println(historyJson);
                    //break;
                }
                
                if (count % 10000 == 0)
                    System.out.println("Records Processed " + count + "(" +dateFormat.format(date));
                count++;
            }
            
            System.out.println("Total Records Processed " + count);
            System.out.println("Total Records written " + recordsInFile);
        }
	}
        protected static String getUri(JsonObject historyJSON)
        {
           String uri = "error";
            if(historyJSON != null)
            {
                if (historyJSON.get("uri") != null)
                {
                    uri=historyJSON.get("uri").getAsString();
                }
            }
           return uri;  
        }
        protected static String getType(JsonObject historyJSON)
        {
             String type = "error";
            if(historyJSON != null)
            {
                if (historyJSON.get("type") != null)
                {
                    type=historyJSON.get("type").getAsString();
                }
            }
           return type; 
        }
        
        protected static String getCustomerID(JsonObject historyJSON)
        {
            String Id = "error";
            JsonElement jElement=historyJSON.getAsJsonObject().get("attributes");
            if (jElement != null)
            {
                JsonElement attribute = jElement.getAsJsonObject().get("EC_Customer_Account_Number");
                if (attribute != null)
                {
                    JsonArray valueArray = attribute.getAsJsonArray();
                    if (valueArray != null && valueArray.size()>0)
                    {
                        Id = valueArray.get(0).getAsJsonObject().get("value").getAsString();
                        //System.out.println(Id);
                    }
                    //String Id = jElement.getAsJsonObject().get("EC_Customer_Account_Number").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
                }
            }
           return Id;
        }
        
        protected static String getRecordChangedByUserName(JsonObject historyJSON)
        {
           String updatedBy = "error";
            if(historyJSON != null)
            {
                if (historyJSON.get("updatedBy") != null)
                {
                    updatedBy=historyJSON.get("updatedBy").getAsString();
                }
            }
           return updatedBy; 
        } 
        
        
        protected static String getRecordUpdateDate(JsonObject historyJSON)
        {
            String updateTime = "error";
            if(historyJSON != null)
            {
                if (historyJSON.get("updatedTime") != null)
                {
                    updateTime=historyJSON.get("updatedTime").getAsString();
                }
            }
           return updateTime; 
        }
}
