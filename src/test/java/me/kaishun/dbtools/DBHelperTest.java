package me.kaishun.dbtools;

import me.kaishun.configuration.ReadeConf;
import org.junit.Test;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class DBHelperTest {

    @Test
    public void testGetConn(){
        ReadeConf.readParameter();
        DBHelper dbHelper = new DBHelper();
        dbHelper.setDBValue(ReadeConf.sqlserverOrMysql,ReadeConf.user,ReadeConf.passwd,ReadeConf.ip,
                ReadeConf.dbName,ReadeConf.port);
        dbHelper.getConn();
    }
    @Test
    public void testExcuteUpdate(){
        ReadeConf.readParameter();
        DBHelper dbHelper = new DBHelper();
        dbHelper.setDBValue(ReadeConf.sqlserverOrMysql,ReadeConf.user,ReadeConf.passwd,ReadeConf.ip,
                ReadeConf.dbName,ReadeConf.port);
        dbHelper.getConn();
        //创建表
        String ddlTableSql = "create table "+ReadeConf.dbName+".tabletest (id varchar(50),name varchar(50))";
        int result = dbHelper.executeUpdate(ddlTableSql);
        System.out.println(result);
    }

    @Test
    public void testPrepareBatchExecute(){
        DBHelper dbHelper = new DBHelper();
        dbHelper.setDBValue("mysql","user","passwd","192.168.x.xx","STUDENT","3306");
        dbHelper.getConn();
        //TODO
    }
}
