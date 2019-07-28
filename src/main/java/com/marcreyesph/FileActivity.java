package com.marcreyesph;

public class FileActivity {
    private String transactionId;
    private String eventType;
    private String path;
    private String ownerName;
    private String cTime;
    private String timeStamp;
    private String fileSize;
    private String dstPath;
    private String srcPath;

    public FileActivity(String transactionId,
                        String eventType,
                        String path,
                        String ownerName,
                        String cTime,
                        String timeStamp,
                        String fileSize,
                        String dstPath,
                        String srcPath) {
        this.transactionId = transactionId;
        this.eventType = eventType;
        this.path = path;
        this.ownerName = ownerName;
        this.cTime = cTime;
        this.timeStamp = timeStamp;
        this.fileSize = fileSize;
        this.dstPath = dstPath;
        this.srcPath = srcPath;
    }

    public FileActivity() {

    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public String getcTime() {
        return cTime;
    }

    public void setcTime(String cTime) {
        this.cTime = cTime;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getFileSize() {
        return fileSize;
    }

    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }

    public String getDstPath() {
        return dstPath;
    }

    public void setDstPath(String dstPath) {
        this.dstPath = dstPath;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }
}