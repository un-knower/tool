package com.hiido.hcat.service;

import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.thrift.protocol.Field;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by zrc on 16-7-12.
 */
public class HcatQuery {
    private static final Logger LOG = Logger.getLogger(HcatQuery.class);
    static final String INIT_SQL = "update bees.hcat_query set state=3,exec_end= where state=1 and ";

    public static enum DbOperation {
        INSERT("insert into bees.hcat_query(qid,commit_month,`user`,committer,state,quick) values(?,?,?,?,?,?)"),
        UPDATE("update bees.hcat_query set state=?,exec_end=?, resourcedir=?, jobIds=?, fieldList=?, resSize=? where qid=?"),
        SELECT("select exec_start, exec_end, state, resourcedir, jobIds, fieldList, isFetchTask, resSize from bees.hcat_query where qid=?");
        String sql;
        private DbOperation(String sql) {
            this.sql = sql;
        }
    }

    private String qid;
    private String user;
    private boolean quick;
    private String committer;
    private Timestamp exec_end;
    private int state;
    private String resourcedir;
    private long resSize;
    private List<String> jobIds;
    private List<Field> fieldList;
    private DbOperation operation;

    public String getQid() {
        return qid;
    }

    public void setQid(String qid) {
        this.qid = qid;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public boolean isQuick() {
        return quick;
    }

    public void setQuick(boolean quick) {
        this.quick = quick;
    }

    public String getCommitter() {
        return committer;
    }

    public void setCommitter(String committer) {
        this.committer = committer;
    }

    public Timestamp getExec_end() {
        return exec_end;
    }

    public void setExec_end(Timestamp exec_end) {
        this.exec_end = exec_end;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getResourcedir() {
        return resourcedir;
    }

    public void setResSize(long resSize) {
        this.resSize = resSize;
    }

    public long getResSize() {
        return resSize;
    }

    public void setResourcedir(String resourcedir) {
        this.resourcedir = resourcedir;
    }

    public List<String> getJobIds() {
        return jobIds;
    }

    public void setJobIds(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    public List<Field> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<Field> fieldList) {
        this.fieldList = fieldList;
    }

    public DbOperation getoperation() {
        return operation;
    }

    public void setOperation(DbOperation operation) {
        this.operation = operation;
    }

    public static java.sql.PreparedStatement createStatement(java.sql.Connection conn, HcatQuery hq) throws SQLException {
        java.sql.PreparedStatement statement = null;
        statement = conn.prepareStatement(hq.operation.sql);
        prepareStatement(statement, hq);
        return statement;
    }

    public static void prepareStatement(java.sql.PreparedStatement statement, HcatQuery hq) throws SQLException {
        Calendar cd = Calendar.getInstance();
        cd.setTimeInMillis(System.currentTimeMillis());
        switch (hq.getoperation()) {
            case INSERT:
                statement.setString(1, hq.qid);
                statement.setInt(2, cd.get(Calendar.MONTH) + 1);
                statement.setString(3, hq.user);
                statement.setString(4, hq.committer);
                statement.setInt(5, hq.state);
                statement.setBoolean(6, hq.quick);
                break;
            case UPDATE:
                statement.setInt(1, hq.state);
                statement.setTimestamp(2, hq.exec_end);
                statement.setString(3, hq.resourcedir);
                StringBuilder builder = new StringBuilder();
                if(hq.jobIds != null)
                    for(String s : hq.jobIds)
                        builder.append(s).append(",");
                statement.setString(4, builder.toString());
                builder.setLength(0);
                if(hq.fieldList != null && hq.fieldList.size() > 0)
                    for(Field f :hq.fieldList)
                        builder.append(f.getName()).append("=").append(f.getType()).append(",");
                statement.setString(5, builder.toString());
                statement.setLong(6, hq.resSize);
                statement.setString(7, hq.qid);

                break;
            case SELECT:
                statement.setString(1, hq.qid);
                break;
        }
    }

    public static List<String> convertJobIds(String jobs) {
        if(jobs != null)
            return Arrays.asList(jobs.split(","));
        else
            return Collections.<String>emptyList();
    }

    public static List<Field> convertFieldList(String qid, String fields) {
        if(StringUtils.isEmpty(fields))
            return Collections.<Field>emptyList();
        try{
            List<Field> fieldList = new LinkedList<Field>();
            for(String str : fields.split(",")) {
                String[] kv = str.split("=");
                fieldList.add(new Field(kv[0], kv[1]));
            }
            return fieldList;
        } catch (Exception e) {
            LOG.error(String.format("failed to convert FieldList string : qid=%s.", qid));
            return Collections.<Field>emptyList();
        }
    }
}
