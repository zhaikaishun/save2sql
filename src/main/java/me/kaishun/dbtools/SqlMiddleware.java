package me.kaishun.dbtools;
import com.sun.rowset.CachedRowSetImpl;
import me.kaishun.configuration.ReadeConf;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;

public class SqlMiddleware {
    public static final String SQLSERVER = "sqlserver";
    private ArrayList<String[]> saveLists = new ArrayList<>();
    private SQLServerBulkTools sqlServerBulkTools;
    private CachedRowSetImpl cachedRowSet;
    private DBHelper dbHelper;
    private String loadSql;

    public void setLoadSql(String loadSql) {
        this.loadSql = loadSql;
    }

    public SqlMiddleware() {}
    public SqlMiddleware(DBHelper dbHelper,SQLServerBulkTools sqlServerBulkTools) {
        this.dbHelper = dbHelper;
        this.sqlServerBulkTools = sqlServerBulkTools;
    }

    /**
     * sqlserver才有，设置CachedRowSetImpl
     */
    public void setCachedRowSet(){
        if(SQLSERVER.equals(ReadeConf.sqlserverOrMysql)){
            try {
                cachedRowSet = sqlServerBulkTools.getCachedRowSet();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void setInsertAndLoadSql(String loadSql) {
        this.loadSql=loadSql;
    }
    public void insertIntoCache(String[] array){
        if(SQLSERVER.equals(ReadeConf.sqlserverOrMysql)){
            try {
                sqlServerBulkTools.insertIntoCachedRowSet(cachedRowSet,array);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            saveLists.add(array);
        }
    }
    public void saveToTable(){
        if(SQLSERVER.equals(ReadeConf.sqlserverOrMysql)){
            try {
                sqlServerBulkTools.insertBatch(cachedRowSet,10000);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else {
            StringBuilder sb = new StringBuilder();
            for (String[] saveBulk : saveLists) {
                for (int i = 0; i <saveBulk.length-1 ; i++) {
                    sb.append(saveBulk[i]).append("\t");
                }
                sb.append(saveBulk[saveBulk.length-1]).append("\n");
            }
            byte[] bytes = sb.toString().getBytes();
            InputStream inputStream = new ByteArrayInputStream(bytes);
            dbHelper.prepareBatchInputStreamExecute(loadSql,inputStream);
            saveLists.clear();
        }

    }


}
