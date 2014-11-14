/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.reltio.data.report;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 *
 * @author admin
 */
public class GetTime {
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
        
        Properties prop=new Properties();
         System.out.println(System.getProperty("user.dir"));
         InputStream inp=new FileInputStream("C:/date.properties");
         prop.load(inp);
         String Date1=prop.getProperty("startDate");
         String Date2 = prop.getProperty("endDate");
            Date dr = new SimpleDateFormat("MM/dd/yyyy").parse(Date1);
            Date dr1 = new SimpleDateFormat("MM/dd/yyyy").parse(Date2);
            long dt1=dr.getTime();
            long dt2=dr1.getTime();
            System.out.println(dt1+"    "+dt2);
    }
}
