package me.kaishun.configuration;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.sun.rowset.CachedRowSetImpl;
import me.kaishun.dbtools.DBHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    public DBHelper dbHelper;
    public String tableName;

    /**
     * 获取空的CachedRowSet对象
     * 其中BaseDao为最常见的JDBC操作,这里就不贴出,相信大家看的懂
     * @throws
     */
    public CachedRowSetImpl getCachedRowSet() throws SQLException {
        //查询出空值用于构建CachedRowSetImpl对象以省去列映射的步骤
        ResultSet rs = dbHelper.getResultSet("select * from "+tableName+" where 1=0",null);
        CachedRowSetImpl crs = new CachedRowSetImpl();
        crs.populate(rs);
        //获取crs以后关闭数据库连接
//        baseDao.closeResource();
        return crs;
    }

    /**
     * 向CachedRowSet对象插入一条数据
     * 循环调用这一个方法，将想插入数据库的数据先插入到CachedRowSet对象里
     * @param crs
     * @param
     * @return
     * @throws SQLException
     */
    public CachedRowSetImpl insertIntoCachedRowSet(CachedRowSetImpl crs,String[] aa) throws SQLException{
        //移动指针到“插入行”，插入行是一个虚拟行
        crs.moveToInsertRow();
        //更新虚拟行的数据
        for (int i = 0; i < aa.length; i++) {
            crs.updateString("v"+(i), aa[i]);
        }
//        crs.updateString("name", stu.getName());
//        crs.updateInt("id", stu.getId());
        //插入虚拟行
        crs.insertRow();
        //移动指针到当前行
        crs.moveToCurrentRow();
        return crs;
    }

    /**
     * 使用BulkCopy和RowSet进行批量插入
     * @param crs
     * @param batchSize
     */
    public void insertBatch(CachedRowSetImpl crs,int batchSize)throws SQLException {
        //数据库连接字符串

//        String sDBUrl = "jdbc:sqlserver://" + ReadeConf.ip+":"+16802 + ";DatabaseName=" + ReadeConf.dbName;
////        String url = "jdbc:sqlserver://localhost\\SQLEXPRESS:16802;DatabaseName=MBD2_CITY_MAIN"
////                + ";user=dtuaser;password=dtauser";
//        String url=sDBUrl+";user="+ReadeConf.user+";password="+ReadeConf.passwd;
//
//        Connection connectionUrl = DriverManager.getConnection(url);
        SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
        copyOptions.setKeepIdentity(true);
        copyOptions.setBatchSize(8000);
        copyOptions.setUseInternalTransaction(false);
        SQLServerBulkCopy bulkCopy =
                new SQLServerBulkCopy(dbHelper.getConn());
        bulkCopy.setBulkCopyOptions(copyOptions);
        bulkCopy.setDestinationTableName(tableName);
        bulkCopy.writeToServer(crs);
        crs.close();
        bulkCopy.close();
    }
}
