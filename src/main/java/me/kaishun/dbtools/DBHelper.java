package me.kaishun.dbtools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class DBHelper {
    private static Logger logger = LogManager.getLogger(DBHelper.class.getName());
    Connection conn = null;
    String sUser;
    String sPwd;
    String serverIp;
    String dbName;
    int port;
    private String sqlserverOrMysql;


    public void setDBValue(String sqlserverOrMysql,String user, String pwd, String ip, String dbName,String port) {
        this.sqlserverOrMysql = sqlserverOrMysql;
        this.sUser = user;
        this.sPwd = pwd;
        this.serverIp = ip;
        this.dbName = dbName;
        this.port = Integer.parseInt(port.trim());
    }


    // 取得连接
    public Connection getConn() {
        if (conn != null){
            return conn;
        }
        try {
            String sDriverName = "";
            String sDBUrl = "";
            if("mysql".contains(sqlserverOrMysql.toLowerCase())){
                sDriverName = "com.mysql.jdbc.Driver";
                sDBUrl = "jdbc:mysql://" + serverIp +":"+port+ "/"+ dbName +"?useUnicode=true&characterEncoding=utf8&useServerPreparedStmts =true";
                Class.forName(sDriverName);
                conn = DriverManager.getConnection(sDBUrl, sUser, sPwd);
            }
            else if("sqlserver".contains(sqlserverOrMysql.toLowerCase())){
                sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                sDBUrl = "jdbc:sqlserver://" + serverIp+":"+port + ";DatabaseName=" + dbName;
                Class.forName(sDriverName);
                conn = DriverManager.getConnection(sDBUrl+";user="+sUser+";password="+sPwd);
            }

        } catch (Exception ex) {
            logger.error("connection error:",ex);
            return null;
        }
        return conn;
    }



    // 关闭连接
    public void closeConn() {
        try {
            conn.close();
            conn = null;
        } catch (Exception ex) {
            logger.error("connect error: ",ex);
            conn = null;
        }
    }

    /**
     * 批量得到result,这里不能关闭
     * @param sSQL
     * @param objParams
     * @return
     */
    public ResultSet getResultSet(String sSQL, Object[] objParams) {
        getConn();
        ResultSet rs = null;
        try {
            PreparedStatement ps = conn.prepareStatement(sSQL);
            if (objParams != null)
            {
                for (int i = 0; i < objParams.length; i++)
                {
                    ps.setObject(i + 1, objParams[i]);
                }
            }
            rs = ps.executeQuery();
        } catch (Exception ex) {
            logger.error("connect error: ",ex);
            closeConn();
        }
        return rs;
    }


    /**
     * DDL的操作，或者INSERT、UPDATE 或 DELETE 语句
     * @param sSQL
     * @return
     */
    public int executeUpdate(String sSQL) {
        getConn();
        int iResult = 0;
        try (Statement ps = conn.createStatement();){
            iResult = ps.executeUpdate(sSQL);
        } catch (Exception ex) {
            logger.error("connect error: ",ex);
            return -1;
        }
        return iResult;
    }


    public void prepareBatchInputStreamExecute(String loadSql, InputStream inputStream){
        getConn();
        try (PreparedStatement preparedStatement = conn.prepareStatement(loadSql);){
            conn.setAutoCommit(false);
            if (preparedStatement.isWrapperFor(com.mysql.jdbc.Statement.class)) {
                com.mysql.jdbc.PreparedStatement mysqlStatement =
                        preparedStatement.unwrap(com.mysql.jdbc.PreparedStatement.class);
                mysqlStatement.setLocalInfileInputStream(inputStream);
                int result = mysqlStatement.executeUpdate();
                conn.commit();
            }
//            ((Statement)preparedStatement).setLocalInfileInputStream(inputStream); //报错，可能引的包不对
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 批量INSERT等语句
     * @param sSQL
     * @param objParams
     */
    public void prepareBatchExecute(String sSQL, ArrayList<String[]> objParams) {
        if(objParams==null||objParams.isEmpty()){
            return;
        }
        getConn();
        try (PreparedStatement preparedStatement = conn.prepareStatement(sSQL);){
            conn.setAutoCommit(false);
            for (int i = 0; i <objParams.size() ; i++) {
                String[] strings = objParams.get(i);
                for (int i1 = 0; i1 < strings.length; i1++) {
                    preparedStatement.setString(i1+1,strings[i1]);
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            conn.commit();
            preparedStatement.clearParameters();
        } catch (Exception ex) {
            logger.error("prePareBatchExecute insert error: ",ex);
        }
    }
}
