package com.spark.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZXL on 2017/10/17.
 */
public class WeblogService {
    static String url ="jdbc:mysql://node7:3306/test?useUnicode=true&characterEncoding=utf-8";
    static String username="root";
    static String password="1234";

    public  Map<String,Object> queryWeblogs() {
        Connection conn = null;
        PreparedStatement pst = null;
        String[] titleNames = new String[20];
        String[] titleCounts = new String[20];
        Map<String,Object> retMap = new HashMap<String, Object>();
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url,username,password);
            String query_sql = "select titleName,count from webCount where 1=1 order by count desc limit 20";
            pst = conn.prepareStatement(query_sql);
            ResultSet rs = pst.executeQuery();
            int i = 0;
            while (rs.next()){
                String titleName = rs.getString("titleName");
                String titleCount = rs.getString("count");
                titleNames[i] = titleName;
                titleCounts[i] = titleCount;
                ++i;
            }
            retMap.put("titleName", titleNames);
            retMap.put("titleCount", titleCounts);
        }catch(Exception e){
            e.printStackTrace();
        }
             return retMap;
    }

    public  String[] titleCount() {
        Connection conn = null;
        PreparedStatement pst = null;
        String[] titleSums = new String[1];
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url,username,password);
            String query_sql = "select count(1) titleSum from webCount";
            pst = conn.prepareStatement(query_sql);
            ResultSet rs = pst.executeQuery();
            if(rs.next()){
                String titleSum = rs.getString("titleSum");
                titleSums[0] = titleSum;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return titleSums;
    }

}
