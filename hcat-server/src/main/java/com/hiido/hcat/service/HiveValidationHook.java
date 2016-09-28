package com.hiido.hcat.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.hiido.suit.Business;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.hiido.hcat.common.PublicConstant;
import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.suit.net.http.protocol.SecurityAuth;
import com.hiido.suit.net.http.protocol.SecurityAuth.AuthEntry;
import com.hiido.suit.service.security.SecurityObject;
import com.hiido.suit.service.security.SecurityObject.PriviType;
import org.apache.log4j.Logger;

public class HiveValidationHook extends AbstractSemanticAnalyzerHook {
    private static final Logger LOG = Logger.getLogger(HiveValidationHook.class);
    private static final String EmptyStr = "";
    private List<String> verifyUDF;
    private HiveConf conf;
    private ASTNode ast;

    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
        this.conf = (HiveConf) context.getConf();
        this.ast = ast;
        if (verifyUDF == null) {
            String udfs = HiveConfConstants.getHcatVerifyUDFs(conf);
            verifyUDF = "".equals(udfs) ? null : Arrays.asList(udfs.split(","));
        }
        return ast;
    }

    public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<? extends Serializable>> rootTasks)
            throws SemanticException {

        HiveOperation op = ((HiveSemanticAnalyzerHookContextImpl)context).getSem().getQueryState().getHiveOperation();
        if (!filterHiveOperation(op))
            return;

        BaseSemanticAnalyzer sem = ((HiveSemanticAnalyzerHookContextImpl) context).getSem();
        List<AuthEntry> authInfo = new LinkedList<AuthEntry>();
        if (sem instanceof FunctionSemanticAnalyzer) {
            if (!(ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null))
                throw new SemanticException("Unsupported create-function syntax, please use create-temporary-function syntax.");
            if (verifyUDF != null) {
                String className = BaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
                if (verifyUDF != null && verifyUDF.contains(className)) {
                    try {
                        AuthEntry entry = SecurityObject
                                .buildAuthEntry4Table(PriviType.QUERY, PublicConstant.METASTORE, "udf", className.replace('.', '_'));
                        SecurityAuth auth = SecurityObject.buildSecurityAuth(SecurityAuth.REQUEST_TYPE_AUTH, "", entry);
                        //a3.A3(auth);
                    } catch (Exception e) {
                        throw new AuthorizationException("failed to a3 of " + e.getClass().getName() + ":" + (e.getMessage() == null ? "" : e.getMessage()), e);
                    }
                }
                return;
            }
        } else if (sem instanceof DDLSemanticAnalyzer) {
            Set<WriteEntity> writeSet = context.getOutputs();
            for (WriteEntity entity : writeSet) {
                if (entity.getType() == Type.TABLE) {
                    AuthEntry entry = new AuthEntry();
                    if (op == HiveOperation.DROPTABLE || op == HiveOperation.DROPVIEW)
                        entry.setPrivi_type(PriviType.DROPTABLE.toString());
                    else
                        entry.setPrivi_type(PriviType.WRITETABLE.toString());
                    entry.setBusi_type(Business.BusType.HIVE.toString());
                    AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
                    String object_name = String.format("%s.%s", "default", entity.getName().replace('@', '.'));

                    objectInfo.setObject_name(object_name);
                    entry.setObject_info(objectInfo);
                    authInfo.add(entry);
                }
            }
        } else {
            ColumnAccessInfo columnAccess = sem.getColumnAccessInfo();
            Set<WriteEntity> writeSet = context.getOutputs();
            Set<ReadEntity> readSet = context.getInputs();

            for (WriteEntity entity : writeSet) {
                if (entity.getType() == Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");
                if (entity.getType() == Type.TABLE) {
                    AuthEntry entry = new AuthEntry();
                    if ((op == HiveOperation.CREATETABLE || op == HiveOperation.CREATETABLE_AS_SELECT))
                        entry.setPrivi_type(PriviType.CREATETABLE.toString());
                    else
                        entry.setPrivi_type(PriviType.WRITETABLE.toString());
                    entry.setBusi_type(Business.BusType.HIVE.toString());
                    AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
                    String table = entity.getName().replace('@', '.');
                    if (table.startsWith(context.getUserName()))
                        continue;
                    String object_name = String.format("%s.%s", "default", table);
                    addColumnInfo(objectInfo, columnAccess, entity.getName());
                    objectInfo.setObject_name(object_name);
                    entry.setObject_info(objectInfo);
                    authInfo.add(entry);
                    if(LOG.isDebugEnabled())
                        LOG.debug(String.format("write entity %s: %s", entity.getTyp().name(), entity.getName()));
                }
                if(entity.getWriteType() == WriteEntity.WriteType.PATH_WRITE) {
                    if(entity.getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR)))
                        continue;
                    AuthEntry entry = new AuthEntry();
                    entry.setPrivi_type(PriviType.WRITEDFS.toString());
                    entry.setBusi_type(Business.BusType.HDFS.toString());
                    AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
                    objectInfo.setObject_name(entity.getD().toUri().getPath());
                    objectInfo.addExtra("DIR");
                    entry.setObject_info(objectInfo);
                    authInfo.add(entry);
                    if(LOG.isDebugEnabled())
                        LOG.debug(String.format("write entity %s: %s", entity.getTyp().name(), entity.getName()));
                }
            }
            for (ReadEntity entity : readSet) {
                if (entity.getType() == Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");

                if (entity.getType() == Type.TABLE && entity.isDirect()) {
                    AuthEntry entry = new AuthEntry();
                    entry.setPrivi_type(PriviType.QUERY.toString());
                    entry.setBusi_type(Business.BusType.HIVE.toString());
                    AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
                    String table = entity.getName().replace('@', '.');
                    if (table.startsWith(context.getUserName()))
                        continue;
                    String object_name = String.format("%s.%s", "default", table);
                    objectInfo.setObject_name(object_name);
                    addColumnInfo(objectInfo, columnAccess, entity.getName());
                    entry.setObject_info(objectInfo);
                    authInfo.add(entry);
                    if(LOG.isDebugEnabled())
                        LOG.debug(String.format("read entity %s: %s", entity.getTyp().name(), entity.getName()));
                }
            }

        }
        if (authInfo.size() == 0)
            return;
        SecurityAuth sa = createSecurityAuth(context.getUserName());
        sa.setAuth_info(authInfo);
        com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient client = new com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient();
        try {
            client.setAddrList(conf.get(PublicConstant.HCAT_AUTHENTICATION_SERVERS));
            client.setClientNum(-1);
            com.hiido.suit.net.http.protocol.HttpApacheClient apacheClient = new com.hiido.suit.net.http.protocol.HttpApacheClient();
            client.setHttpProtocolClient(apacheClient);

            SecurityAuth.Reply reply = client.post(sa, SecurityAuth.Reply.class);
            if (reply == null || !"success".equals(reply.getResp_code())) {
                throw new AuthorizationException("fail to auth:" + reply == null ? "NULL" : reply.toString());
            }
        } catch (Exception e) {
            if (e instanceof AuthorizationException)
                throw (AuthorizationException) e;
            LOG.error("failed to connect authorization Server.", e);
            throw new AuthorizationException(String.format("failed to connect authorization Server : %s", e.getMessage()));
        } finally {
            client.close();
        }
    }

    private void addColumnInfo(AuthEntry.ObjectInfo objectInfo, ColumnAccessInfo columnAccess, String tableName) {
        if (columnAccess != null) {
            List<String> columns = columnAccess.getTableToColumnAccessMap().get(tableName);
            if (columns != null && columns.size() > 0)
                for (String s : columns)
                    objectInfo.addExtra(s);
            else
                objectInfo.addExtra("*");
        }
    }


    protected SecurityAuth createSecurityAuth(String bususer) {
        SecurityAuth sa = new SecurityAuth();
        sa.setClient_user_name("superman");
        sa.setClient_passwd("032ce83b465499938dhg77d8bc9ef7fc");
        sa.setVersion("1.02");
        sa.setRequest_type(SecurityAuth.REQUEST_TYPE_AUTH);
        sa.setUser_name(bususer);
        sa.setLog_user(EmptyStr);
        sa.setCur_user(EmptyStr);
        sa.setQuery_id(conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
        sa.setSid(EmptyStr);
        sa.setQstring(EmptyStr);
        return sa;
    }

    public static boolean filterHiveOperation(HiveOperation op) {
        switch (op) {
            case EXPLAIN:
            case SWITCHDATABASE:
            case DESCFUNCTION:
            case SHOWDATABASES:
            case SHOWTABLES:
            case SHOWCOLUMNS:
            case SHOW_TABLESTATUS:
            case SHOW_TBLPROPERTIES:
            case SHOWFUNCTIONS:
            case SHOWINDEXES:
            case SHOWCONF:
                return false;
            case DROPDATABASE:
                throw new AuthorizationException("hcat not support drop database.");
            default:
                return true;
        }
    }
}
