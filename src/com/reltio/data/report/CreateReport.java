/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.reltio.data.report;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import com.google.gson.*;
import java.net.URLEncoder;
import java.text.ParseException;

public class CreateReport {

    private static String username = null;
    private static String password = null;
    private static String token = null;
    private static String totalReport2 = null;
    private static String totalUpdatedByAllUser=null;
    private static Gson gson = new Gson();
    private static Map<String, String> dataReport3_4 = new HashMap<String, String>();
    private static Map<String, String> dataReport2 = new HashMap<String, String>();
    private static List<String> keyNameReport3 = new ArrayList<String>();
    private static List<String> keyNameReport4 = new ArrayList<>();
    private static String apiUrl = null;
    private static String dbscanURL = null;
    private static String entityName = null;
    private static Properties configProperties = new Properties();
    private static Properties usersProperties = new Properties();
    private static Properties dateRangeProperties = new Properties();
    private static String saveReportSource=null;

    private static void init(String srcConfig, String srcUserList, String srcDateRange) throws FileNotFoundException, IOException {
        loadProperties(configProperties, srcConfig);
        loadProperties(usersProperties, srcUserList);
        loadProperties(dateRangeProperties, srcDateRange);
    }

    public static void main(String[] args) throws Exception {
        init(args[0], args[1], args[2]);

        /*      
         args[0]=path of configuration file
         args[1]=path of userConfiguration file
         args[2]=path of date range file
         */
        System.out.println(System.getProperty("user.dir"));
        apiUrl = configProperties.getProperty("apiUrl");
        username = configProperties.getProperty("username");
        password = configProperties.getProperty("password");
        saveReportSource=configProperties.getProperty("saveFileSource");
        boolean drRange=getDateRange();{
        if(drRange==true){
            
        
        boolean ret = getAccessToken(username, password);
        System.out.println(ret + "   " + token);
        if (ret) {
            GenerateReport2(usersProperties, apiUrl, username, password);
            GenerateReportOneThreeAndFour(usersProperties, apiUrl, username, password);
        }
        System.out.println("Program ended");
    }else{
            System.err.println("Check Yopur date range");
        }
        }
    }

    public static void loadProperties(Properties properties, String fileName) throws IOException {
        InputStream config = new FileInputStream(fileName);
        properties.load(config);
    }
public static boolean getDateRange() throws ParseException{
    String Date1 = dateRangeProperties.getProperty("startDate");
            String Date2 = dateRangeProperties.getProperty("endDate");
            Date dr = new SimpleDateFormat("MM/dd/yyyy").parse(Date1);
            Date dr1 = new SimpleDateFormat("MM/dd/yyyy").parse(Date2);
            if(dr.before(dr1)){
                return true;
            }
            else {
                return  false;
            }
            
}
    public static void GenerateReportOneThreeAndFour(Properties usersFile, String apiUrl, String username, String password) throws ParseException {

        try {
            long startTime = System.currentTimeMillis();

            String startDate = dateRangeProperties.getProperty("startDate");
            String endDate = dateRangeProperties.getProperty("endDate");
            Date dr = new SimpleDateFormat("MM/dd/yyyy").parse(startDate);
            Date dr1 = new SimpleDateFormat("MM/dd/yyyy").parse(endDate);
            Long start_date = dr.getTime();
            Long end_date = dr1.getTime();
           

            Enumeration e = usersFile.propertyNames();
            
            String url = generateUserSpecificFilter(e, usersFile);

            System.out.println(url);
            // https://sndbx.reltio.com/reltio/api/sy01chd01/entities?_totalfilter=equals(type,'configuration/entityTypes/SoldToCustomer') and (equals(updatedBy,'johnson.alixandra') or equals(updatedBy,'cormier.haley') or equals(updatedBy,'bilikha.emmanuel') or equals(updatedBy,'lewis.keisha') or equals(updatedBy,'william.chu') or equals(updatedBy,'andrew.edwards') or equals(updatedBy,'frazier.terietta') or equals(updatedBy,'medina.julian') or equals(updatedBy,'muhammad.zahid') or equals(updatedBy,'iyok.akwensioge') or equals(updatedBy,'colton.debra') or equals(updatedBy,'ebony.williams') or equals(updatedBy,'semaski.james') or equals(updatedBy,'dominguez.santanna') or equals(updatedBy,'al-daffaie.duraid') or equals(updatedBy,'huynh.karen') or equals(updatedBy,'pedreguera.josue')) and range(updatedTime,1414261800000,1414521000000)

            keyNameReport3.add("Total number of \"Audited (User Data)\" records");
            keyNameReport3.add("Total number of \"Audited (CHD_Data)\" records");
            keyNameReport3.add("Total number of \"Audit Failed\" records");
            keyNameReport3.add("Total number of \"Audited (CHD_Matched)\" records");
            keyNameReport4.add("Total number of \"Escalation\" records");
            keyNameReport4.add("Total number of \"Escalation Completed\" records");
            int j = 2;

            String json1 = "{\"type\" : \"configuration/entityTypes/" + entityName + "\",\"pageSize\" : 150 }";
            for (String key : keyNameReport3) {

                String status = "EC_Audit_Status";

                String total = getTotalAsString(j, apiUrl, status, start_date, end_date, username, password, json1);
                dataReport3_4.put(key, total);
                j++;
            }
            System.out.println(" Report 3\n" + dataReport3_4);
            int k = 2;
            for (String key : keyNameReport4) {

                String status = "EC_Status";
                String total = getTotalAsString(k, apiUrl, status, start_date, end_date, username, password, json1);
                dataReport3_4.put(key, total);
                k++;
            }
            System.out.println(" Report 4\n" + dataReport3_4);
            String urlReport1 = apiUrl + "/entities/_total?filter=" + URLEncoder.encode("equals(type,'configuration/entityTypes/SoldToCustomer') and (" + url + ") and range(updatedTime," + start_date + "," + end_date + ")", "utf-8");
            //String urlReport1 = apiUrl + "/entities/_total?filter=equals(type,'configuration/entityTypes/SoldToCustomer') and (" + url + ") and range(updatedTime," + start_date + "," + end_date + ")";
            
            System.out.println("ALL USER"+"  "+startDate+"   "+endDate+" "+urlReport1);
            //https://sndbx.reltio.com/reltio/api/sy01chd01/entities/_total?filter=equals(type,'configuration/entityTypes/SoldToCustomer') and equals(attributes.EC_Audit_Status,'4') and range(updatedTime,1414348200000,1414521000000)
            //https://sndbx.reltio.com/reltio/api/sy01chd01/entities/_total?filter=equals(type,'configuration/entityTypes/SoldToCustomer') and equals(attributes.EC_Status,'2') and range(updatedTime,1414348200000,1414521000000)
            String initialResponse1 = dbScan(json1, urlReport1, username, password);

            long responseTime1 = System.currentTimeMillis();
            long timeRequiredToGetResponse1 = responseTime1 - startTime;
            System.out.println("Response Time : " + (1 / 1000) + " secs");
            System.out.println(timeRequiredToGetResponse1);
            System.out.println("Initial response:\n");
            System.out.println(initialResponse1);

            JsonObject responseToJsonObject1 = gson.fromJson(initialResponse1, JsonObject.class);

             totalUpdatedByAllUser = responseToJsonObject1.get("total").getAsString();
            String stDt = startDate.replaceAll("/", "");
            String enDt = endDate.replaceAll("/", "");
            String outputFileName = saveReportSource+"/Report1_3_4From_" + stDt + "_To_" + enDt + ".csv";
            String header = "Start Date," + startDate + "\n" + "End Date," + endDate + "\n" + "Total number of records updated by all data stewards" + ",";
            BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(outputFileName));

            saveReportOneThreeAndFourAsCsv(header, outputFileWriter);
           } catch (IOException e1) {
            e1.printStackTrace();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    public static String getTotalAsString(int i, String apiUrl, String status, long start_date, long end_date, String username, String password, String json1) {
        String data = null;
        try {
            if (status.equals("EC_Audit_Status")) {
                String urlReport1_3 = apiUrl + "/entities/_total?filter=" + URLEncoder.encode(" equals(type,'configuration/entityTypes/SoldToCustomer') and equals(attributes.EC_Audit_Status,'" + i + "') and range (updatedTime," + start_date + "," + end_date + ")", "utf-8");

                System.out.println(urlReport1_3);

                String initialResponse1_3 = dbScan(json1, urlReport1_3, username, password);
                JsonObject responseToJsonObject1_3 = gson.fromJson(initialResponse1_3, JsonObject.class);
                data = responseToJsonObject1_3.get("total").getAsString();
            }
            if (status.equals("EC_Status")) {

                String urlReport1_4 = apiUrl + "/entities/_total?filter=" + URLEncoder.encode(" equals(type,'configuration/entityTypes/SoldToCustomer') and equals(attributes.EC_Status,'" + i + "') and range (updatedTime," + start_date + "," + end_date + ")", "utf-8");

                System.out.println(urlReport1_4);

                String initialResponse1_4 = dbScan(json1, urlReport1_4, username, password);
                JsonObject responseToJsonObject1_4 = gson.fromJson(initialResponse1_4, JsonObject.class);
                data = responseToJsonObject1_4.get("total").getAsString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return data;
    }

    public static void GenerateReport2(Properties usersFile, String apiUrl, String username, String password) {

        try {

            String startDate = dateRangeProperties.getProperty("startDate");
            String endDate = dateRangeProperties.getProperty("endDate");
            Date dr = new SimpleDateFormat("MM/dd/yyyy").parse(startDate);
            Date dr1 = new SimpleDateFormat("MM/dd/yyyy").parse(endDate);
            Long start_date = dr.getTime();
            Long end_date = dr1.getTime();
            Enumeration e2 = usersFile.propertyNames();
            StringBuilder user = new StringBuilder();
            StringBuilder tot = new StringBuilder();
            String stDt = startDate.replaceAll("/", "");
            String enDt = endDate.replaceAll("/", "");
            String outputFileName = saveReportSource+"/Report2_From_" + stDt + "_To_" + enDt + ".csv";

            System.out.println(startDate + "   " + endDate);
            BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(outputFileName));
            String header = "Records Updated By Data Stewards\n";
            header = header + "Start Date," + startDate + "\n" + "End Date," + endDate + "\n" + "Data Steward ID,Records Updated\n";

            while (e2.hasMoreElements()) {

                String key = (String) e2.nextElement();
                System.out.println(key + " -- " + usersFile.getProperty(key));

                String user_id = usersFile.getProperty(key);
                user.append(user_id);

                dbscanURL = apiUrl + "/entities/_total?filter=" + URLEncoder.encode("(equals(type,'configuration/entityTypes/SoldToCustomer') and (equals(updatedBy,'" + user_id + "')) and range (updatedTime," + start_date + "," + end_date + "))", "utf-8");
                // "https://sndbx.reltio.com/reltio/api/sy01chd01/entities/_total?filter=" + URLEncoder.encode("(equals(type,'configuration/entityTypes/SoldToCustomer'))","utf-8");

                System.out.println(dbscanURL);
                String json = "{\"type\" : \"configuration/entityTypes/" + entityName + "\",\"pageSize\" : 150 }";
                try {
                    String initialResponse = dbScan(json, dbscanURL, username, password);
                    long responseTime = System.currentTimeMillis();
                    long startTime = System.currentTimeMillis();
                    long timeRequiredToGetResponse = responseTime - startTime;
                    System.out.println("Response Time : " + (timeRequiredToGetResponse / 1000) + " secs");
                    System.out.println("Initial response:\n");
                    System.out.println(initialResponse);
                    JsonObject responseToJsonObject = gson.fromJson(initialResponse, JsonObject.class);
                    totalReport2 = responseToJsonObject.get("total").getAsString();
                    tot.append(String.valueOf(totalReport2));

                    System.out.println(totalReport2);
                    long endTime = System.currentTimeMillis();
                    long timeDiff = endTime - startTime;
                    System.out.println("Total time taken : " + (timeDiff / 1000) + " secs");
                } catch (Exception e) {
                }
                dataReport2.put(user_id, totalReport2);
            }
            saveReportTwoAsCsv(header, outputFileWriter);
        } catch (ParseException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveReportTwoAsCsv(String header, BufferedWriter outputFileWriter) throws IOException {
        outputFileWriter.write(header + "\n");
        System.out.println(dataReport2);
        for (Map.Entry<String, String> entry : dataReport2.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            outputFileWriter.write(key + "," + value + "\n");
            outputFileWriter.flush();
        }
    }

    public static void saveReportOneThreeAndFourAsCsv(String header,  BufferedWriter outputFileWriter) throws IOException {

        outputFileWriter.write(header);
        outputFileWriter.write(totalUpdatedByAllUser+ "\n");
        for (Map.Entry<String, String> entry : dataReport3_4.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            {
                outputFileWriter.write(key + "," + value);

                outputFileWriter.write("\n");
                outputFileWriter.flush();
            }
            System.out.println(key + "  " + value);
        }
        System.out.println("Data Map:   " + dataReport3_4);
        outputFileWriter.flush();
        totalUpdatedByAllUser=null;
    }

    public static String dbScan(String json, String surl, String username, String password) throws Exception {

        String srcUrl = surl;
        URL url = new URL(srcUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoInput(true);
        connection.setDoOutput(true); // Triggers POST.   AN
        connection.setRequestProperty("Authorization", "Bearer" + token);
        connection.setRequestProperty("Content-Type", "application/json");

        InputStream response = null;
        StringBuilder builder = new StringBuilder();

        int iteration = 0;
        int errorCode = connection.getResponseCode();
        while (errorCode >= 400) {
            iteration++;
            System.out.println("ErrorCode:" + errorCode + "|Iteration:" + iteration + "|" + Thread.currentThread().getName() + "|");
            if (iteration == 5) {
                System.out.println("FailedJsonToResend_Exception:" + json);
                break;
            }
            getAccessToken(username, password);
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestProperty("Authorization", "Bearer" + token);
            connection.setRequestProperty("Content-Type", "application/json");

            errorCode = connection.getResponseCode();

        }

        try {
            response = connection.getInputStream();

            byte[] buffer = new byte[100000];
            int count = response.read(buffer);
            while (count > 0) {
                builder.append(new String(buffer, 0, count));
                count = response.read(buffer);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }

        return builder.toString();

    }

    public static boolean getAccessToken(String username, String password) throws Exception {

        URLConnection connection = new URL("https://auth02.reltio.com/oauth/token?username=" + username + "&password=" + password + "&grant_type=password").openConnection();
        connection.setRequestProperty("Authorization", "Basic cmVsdGlvX3VpOm1ha2l0YQ==");

        InputStream response = null;
        StringBuilder builder = new StringBuilder();
        try {
            response = connection.getInputStream();

            byte[] buffer = new byte[100000];
            int count = response.read(buffer);

            while (count > 0) {
                builder.append(new String(buffer, 0, count));
                count = response.read(buffer);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }

        String responseStr = builder.toString();
        int index = responseStr.indexOf("\"access_token\":\"");
        if (index >= 0) {
            int endIndex = responseStr.indexOf("\"", index + "\"access_token\":\"".length());
            String access_token = responseStr.substring(index + "\"access_token\":\"".length(), endIndex);
            token = access_token;
            return true;
        } else {
            return false;
        }
    }

    protected static String generateUserSpecificFilter(Enumeration users, Properties usersFile) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        while (users.hasMoreElements()) {
            if (count > 0) {
                sb.append(") or ");
            }
            count++;
            String key = (String) users.nextElement();

            String user = usersFile.getProperty(key);

            sb.append("equals(updatedBy,'");

            sb.append(user);
            sb.append("'");
        }
        sb.append(")");
        return sb.toString();
    }

}
