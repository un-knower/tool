package com.hiido.hcat;

/**
 * Created by zrc on 16-10-13.
 */
public class CompanyInfo {
    private int id;
    private String name;
    private String jobQueue;
    private String hdfs;
    private long updateTime;

    public CompanyInfo() {}

    public CompanyInfo(int id, String name, String jobQueue, String hdfs) {
        this.id = id;
        this.name = name;
        this.jobQueue = jobQueue;
        this.hdfs = hdfs;
        updateTime = System.currentTimeMillis();
    }


    public String getHdfs() {
        return hdfs;
    }

    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJobQueue() {
        return jobQueue;
    }

    public void setJobQueue(String jobQueue) {
        this.jobQueue = jobQueue;
    }

    public void setUpdateTime() {
        updateTime = System.currentTimeMillis();
    }

    public long getUpdateTime() {
        return updateTime;
    }

}
