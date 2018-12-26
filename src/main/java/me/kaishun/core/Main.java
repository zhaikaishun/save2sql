package me.kaishun.core;

import me.kaishun.configuration.ReadeConf;
import me.kaishun.dbtools.SQLServerBulkTools;
import me.kaishun.dbtools.DBHelper;
import me.kaishun.dbtools.SqlMiddleware;
import me.kaishun.filetools.BaseReader;
import me.kaishun.filetools.FileReader;
import me.kaishun.filetools.ReaderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class Main {
    private static Logger logger = LogManager.getLogger(Main.class.getName());
    //文件路径：必填
    @Option(required = true,name="-inputPath",usage = "输入路径")
    private static String inputPath ="";

    //处理类型，选填，默认能解析文本文件，gz压缩文件，如果是读hdfs需要写hdfs
    @Option(name="-sourceType",usage = "hdfs or text or empty")
    private static String sourceType = "text";

    //分隔符，选填，程序能自动识别
    @Option(name="-separator",usage = "separator")
    private static String separator ="";

    //表名，选填，默认为路径的最后一个name+时间
    @Option(name="-tableName",usage = "tableName")
    private static String tableName="";

    //判断是否是sqlserver
    private static String  dbo="";

    public static void main(String[] args) {
        //加载配置，获取参数
        makeconf(args);
        //连接数据库
        DBHelper dbHelper = new DBHelper();
        dbHelper.setDBValue(ReadeConf.sqlserverOrMysql,ReadeConf.user,ReadeConf.passwd,ReadeConf.ip,
                ReadeConf.dbName,ReadeConf.port);
        dbHelper.getConn();
        SQLServerBulkTools sqlServerBulkTools = new SQLServerBulkTools(dbHelper,ReadeConf.dbName+".dbo."+tableName);
        SqlMiddleware sqlMiddleware = new SqlMiddleware(dbHelper,sqlServerBulkTools);

        //工厂方法得到读取文件的的具体的实例化对象
        ReaderFactory readerFactory = new ReaderFactory();
        BaseReader reader = readerFactory.getReader(sourceType);
        reader.setFilePath(inputPath);

        reader.setLinehandler(new FileReader.LineHandler() {
            //记录遍历到了第几行
            private int index=0;
            //记录第一行的内容，需要用第一行和第二行对比，选择长度最长的一行作为表的字段
            private String[] firstLineArray;
            //表的字段的个数
            private int maxLength=0;
            private double allTimeSpend=0;
            /**
             * 回调方法处理每一行数据，并且入库
             * 1. 自动识别分隔符
             * 2. 通过前两行得到数据库的组多的字段
             * 3. 创建数据库
             * 4. 向数据库插入数据
             * @param line 文件中一行一行的数据
             */
            @Override
            public void handleOneLine(String line) {
                if(index==0){
                    separator =getSplitSign(line);
                    //创建一个数组，创建一个表
                    firstLineArray = line.split(separator);
                } else if(index==1){
                    String[] split = line.split(separator);
                    maxLength=Math.max(firstLineArray.length,split.length);
                    //创建表
                    String  loadSql = getLoadSql(maxLength);
                    sqlMiddleware.setInsertAndLoadSql(loadSql);
                    String createTableSql = getCreateTableSql(maxLength);
                    dbHelper.executeUpdate(createTableSql);
                    sqlMiddleware.setCachedRowSet();

                    //缓存第一行和数据
                    String[] firstLine = new String[maxLength];
                    for (int i = 0; i < this.firstLineArray.length; i++) {
                        if(i<maxLength){
                            firstLine[i]= this.firstLineArray[i];
                        }
                    }
                    sqlMiddleware.insertIntoCache(firstLine);
                    //缓存第二行数据
                    String[] secondLine = new String[maxLength];
                    for (int i = 0; i <split.length ; i++) {
                        if(i<maxLength){
                            secondLine[i]=split[i];
                        }
                    }
                    sqlMiddleware.insertIntoCache(secondLine);
                } else if(index>1){
                    String[] split = line.split(separator);
                    String[] tableArray = new String[maxLength];
                    for (int i = 0; i < split.length; i++) {
                        if(i<maxLength){
                            tableArray[i]=split[i];
                        }
                    }
                    sqlMiddleware.insertIntoCache(tableArray);
                }
                index++;
                //每10000行保存一次
                if(index%10000==0){
                    long t1 = System.currentTimeMillis();
                    sqlMiddleware.saveToTable();
                    long t2 = System.currentTimeMillis();
                    allTimeSpend+=(t2-t1)/1000.0;
                    logger.info("handle: "+index+"rows,: save time spend: "+allTimeSpend+"s");
                }
            }

            @Override
            public void dealLastLists() {
                sqlMiddleware.saveToTable();
            }

        });

        try {
            reader.readFile();
        } catch (Exception e) {
            logger.error("haddle error",e);
        }

    }



    /**
     * 加载配置文件
     */
    private static void makeconf(String[] args) {
        //解析命令行传入的参数
        Main main = new Main();
        CmdLineParser cmdLineParser = new CmdLineParser(main);
        try {
            cmdLineParser.parseArgument(args);
        } catch (CmdLineException e) {
            logger.error("cmdLine parse error",e);
        }
        //加载parameter.conf配置
        ReadeConf.readParameter();
        if("sqlserver".equals(ReadeConf.sqlserverOrMysql)){
            dbo=".dbo";
        }

        //如果没有输入表名，那么默认的表名为文件（文件夹）名称+时间
        String fileNameFormat = new File(inputPath).getName().replace(".", "").replace("$", "")
                .replace("#", "").replace("*", "").replace("(", "").replace(")", "").replace(" ", "");
        String defaultName = fileNameFormat+"_"+new SimpleDateFormat("MMddHHmmss").format(new Date());
        if(tableName.length()==0){
            tableName = defaultName;
        }
    }

    private static String getCreateTableSql(int maxLength) {
        String createTableSql = "create table "+ReadeConf.dbName+dbo+"."+tableName+" (";
        for (int i = 0; i <maxLength ; i++) {
            createTableSql = createTableSql+"v"+i+" varchar(255),";
        }
        createTableSql = createTableSql.substring(0, createTableSql.length() - 1)+")";
        return createTableSql;
    }


    private static String getLoadSql(int maxLength) {
        String sql = "LOAD DATA LOCAL INFILE 'no-used.txt' IGNORE INTO TABLE "+ReadeConf.dbName+dbo+"."+tableName+" (";
        for (int i = 0; i < maxLength; i++) {
            sql= sql + "v"+i+",";
        }
        sql = sql.substring(0, sql.length() - 1);
        sql = sql+")";
        String utf8="UTF-8";
        Charset charset = Charset.defaultCharset();
        if(!charset.displayName().equals(utf8)){
            byte[] data = new byte[0];
            try {
                data = sql.getBytes(utf8);
            } catch (UnsupportedEncodingException e) {
                logger.error("utf8 charset error",e);
            }
            sql = new String(data,charset);
        }
        return sql;
    }

    /**
     * 看哪种字符出现的多，就返回哪种字符，用简单的split方法即可
     * @param line
     * @return
     */
    private static String getSplitSign(String line) {
        if(separator.length()>0){
            return separator;
        }
        int length1 = line.split("\t").length;
        int length2 = line.split("\\|").length;
        int length3 = line.split(",").length;
        int length4 = line.split("\\S").length;//todo 大小写问题
        if(length1==1&&length2==1&&length3==1&&length4>3){
            //看是不是空格，空格作为分隔符本来就不好，所以我这边规定要比较大才行，至少为4
            return "\\S";
        }
        int max = Math.max(Math.max(length1, length2), length3);
        return max==length1?"\t":max==length2?"\\|":",";
    }
}
