package me.kaishun.filetools;

import java.io.IOException;

/**
 * Created by zhaikaishun on 2018\12\24 0024.
 */
public abstract class BaseReader {
    private FileReader.LineHandler linehandler;
    public FileReader.LineHandler getLinehandler() {
        return linehandler;
    }
    public void setLinehandler(FileReader.LineHandler linehandler) {
        this.linehandler = linehandler;
    }

    private String filePath;
    public String getFilePath() {
        return filePath;
    }
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
    public abstract void readFile() throws IOException;
}
