package me.kaishun.filetools;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class TextReader extends BaseReader {
    private static Logger logger = LogManager.getLogger(TextReader.class.getName());
    @Override
    public void readFile(){
        logger.info("getFilePath is:"+getFilePath());
        File files = new File(getFilePath());
        if(files.isFile()){
            doSignalFile(getFilePath());
        }else {
            String[] filelist = files.list();
            for (int i = 0; i < filelist.length; i++) {
                String ipath = getFilePath() + File.separator + filelist[i];
                try {
                    doSignalFile(ipath);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * @param ipath
     */
    private void doSignalFile(String ipath) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(ipath);
            if(ipath.endsWith("gz")){
                inputStream = new GZIPInputStream(inputStream);
            }
            FileReader.readFile(inputStream,getLinehandler());
            inputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(inputStream!=null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                inputStream=null;
            }
        }
    }
}
