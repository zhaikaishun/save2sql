package me.kaishun.filetools;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class ReaderFactory {
    public ReaderFactory(){
    }
    public BaseReader getReader(String type){

        if(type.toLowerCase().contains("hdfs")){
            return new HdfsReader();

        }else if(type.toLowerCase().contains("text")){
            return new TextReader();
        }else if(type.toLowerCase().contains("excel")){
            return new ExcelReader();
        }
        return null;
    }

}
