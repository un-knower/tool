//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.hiido.hcat.service;

import java.sql.Timestamp;

public class BeeQuery {
    private static final String SQL_COMMIT = "insert into bees.bee_query(qid,user,query,quick,committer,committime,executor,exec_start,state) values(?,?,?,?,?,?,?,?,?)";
    public static final String SQL_UPDATE = "update bees.bee_query set  state=?,exec_end=?, resourcedir=? where qid=? ";
    private static final String SQL_STATUS = " select exec_start, exec_end, state,jobid,n,res,fields,set_time,errcode,errmsg,ressize from bees.bee_query left join bees.bee_running on bees.bee_query.qid=bees.bee_running.qid where bees.bee_query.qid=?";
    private String qid;
    private String user;
    private String query;
    private int quick;
    private String committer;
    private Timestamp committime;
    private String executor;
    private Timestamp exec_start;
    private Timestamp exec_end;
    private int state;
    private int tocancel;
    private String resourcedir;
    private String commitSQL;
    private boolean insert;

    public BeeQuery() {
    }

    public String getQid() {
        return this.qid;
    }

    public void setQid(String qid) {
        this.qid = qid;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getQuick() {
        return this.quick;
    }

    public void setQuick(int quick) {
        this.quick = quick;
    }

    public String getCommitter() {
        return this.committer;
    }

    public void setCommitter(String committer) {
        this.committer = committer;
    }

    public Timestamp getCommittime() {
        return this.committime;
    }

    public void setCommittime(Timestamp committime) {
        this.committime = committime;
    }

    public String getExecutor() {
        return this.executor;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    public Timestamp getExec_start() {
        return this.exec_start;
    }

    public void setExec_start(Timestamp exec_start) {
        this.exec_start = exec_start;
    }

    public Timestamp getExec_end() {
        return this.exec_end;
    }

    public void setExec_end(Timestamp exec_end) {
        this.exec_end = exec_end;
    }

    public int getState() {
        return this.state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public int getTocancel() {
        return this.tocancel;
    }

    public void setTocancel(int tocancel) {
        this.tocancel = tocancel;
    }

    public String getResourcedir() {
        return this.resourcedir;
    }

    public void setResourcedir(String resourcedir) {
        this.resourcedir = resourcedir;
    }

    public void setInsert(boolean b) {
        this.insert = b;
    }

    public boolean isInsert() {
        return this.insert;
    }

    public String getCommitSQL() {
        return this.insert?"insert into bees.bee_query(qid,user,query,quick,committer,committime,executor,exec_start,state) values(?,?,?,?,?,?,?,?,?)":"update bees.bee_query set  state=?,exec_end=?, resourcedir=? where qid=? ";
    }
}
