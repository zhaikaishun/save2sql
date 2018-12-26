package me.kaishun.filetools;

import me.kaishun.configuration.ReadeConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class HdfsReader extends BaseReader {

    @Override
    public void readFile() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", ReadeConf.fsDefaultFS);

        FileSystem fs = FileSystem.get(conf);
        if(getFilePath().length()==0){
            throw new FileNotFoundException("inputFile is empty");
        }
        Path file = new Path(getFilePath());
        if(!fs.exists(file)) {
            throw new FileNotFoundException(getFilePath());
        }
        FileStatus fileStatus1 = fs.getFileStatus(file);
        if(fileStatus1.isFile()){
            FSDataInputStream fsDataInputStream = fs.open(fileStatus1.getPath());
            FileReader.readFile(fsDataInputStream,getLinehandler());
        }else{
            FileStatus[] fileStatuses = fs.listStatus(file);
            for (FileStatus fileStatus : fileStatuses) {
                if(fileStatus.isFile()){
                    FSDataInputStream fsDataInputStream = fs.open(fileStatus.getPath());
                    FileReader.readFile(fsDataInputStream,getLinehandler());
                }
            }
        }
    }
}
