package me.kaishun.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class ReadeConf {
    private ReadeConf(){

    }

    //hdfs的ip和端口
    public static String fsDefaultFS="";
    //数据库用户名
    public static String user="";
    //数据库密码
    public static String passwd="";
    //数据库IP
    public static String ip="";
    //数据库端口
    public static String port="";
    //hive或者sqlserver
    public static String sqlserverOrMysql ="";
    //数据库名称
    public static String dbName="";

    /**
     * 读取程序内部的parameter.conf文件
     * 获取到fs.defaultFS以及数据库的连接信息
     */
    public static void readParameter(){
        InputStream inputStream = ReadeConf.class.getClassLoader().getResourceAsStream("parameter.conf");
        Properties prop = new Properties();
        try {
            prop.load(inputStream);
            inputStream.close();
            fsDefaultFS = prop.getProperty("fs.defaultFS");
            user = prop.getProperty("user");
            passwd = prop.getProperty("passwd");
            port = prop.getProperty("port");
            sqlserverOrMysql = prop.getProperty("sqlserverOrMysql");
            ip = prop.getProperty("ip");
            dbName = prop.getProperty("dbName");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
