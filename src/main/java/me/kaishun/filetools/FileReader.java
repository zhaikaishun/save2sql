package me.kaishun.filetools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public class FileReader {
    private static Logger logger = LogManager.getLogger(FileReader.class.getName());
    private FileReader(){}

    public interface LineHandler{
        //每一行数据的处理
        void handleOneLine(String line);
        //最后对没有保存到数据库的数据，进行一次保存
        void dealLastLists();
    }

    /**
     *
     * @param inputStream
     * @param linehandler
     * @return
     */
    public static void readFile(InputStream inputStream, LineHandler linehandler) throws IOException {
        InputStreamReader isr = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        while ((line = br.readLine()) != null){
            try{
                linehandler.handleOneLine(line);
            }catch (Exception e){
                logger.error("handleOneLine error",e);
            }
        }
        linehandler.dealLastLists();
        }
}
