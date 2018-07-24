package com.hiido.hcat.service;

import com.hiido.hcat.HcatConstantConf;
import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.hva.thrift.protocol.*;
import com.hiido.hva.thrift.protocol.RuntimeException;
import com.hiido.suit.Business;
import com.hiido.suit.net.http.protocol.SecurityAuth;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import java.io.Serializable;
import java.util.*;

/**
 * Created by zrc on 16-10-8.
 */
public class HvaOnlyHook extends AbstractSemanticAnalyzerHook {

    private static final Logger LOG = Logger.getLogger(HvaHook.class);
    private static final String EmptyStr = "";
    private HiveConf conf;
    private ASTNode ast;
    private List<String> verifyUDF;

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
        this.conf = (HiveConf) context.getConf();
        this.ast = ast;
        if (verifyUDF == null) {
            String udfs = HiveConfConstants.getHcatVerifyUDFs(conf);
            verifyUDF = "".equals(udfs) ? null : Arrays.asList(udfs.split(","));
        }
        return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<? extends Serializable>> rootTasks)
            throws SemanticException {
        HiveOperation op = ((HiveSemanticAnalyzerHookContextImpl) context).getSem().getQueryState().getHiveOperation();
        if (!filterHiveOperation(op))
            return;

        SessionState ss = SessionState.get();
        HiidoUser hiidoUser = new HiidoUser(ss.getHiidoUserId(), ss.getHiidoCompanyId());

        Set<Obj> authSet = new HashSet<Obj>();

        BaseSemanticAnalyzer sem = ((HiveSemanticAnalyzerHookContextImpl) context).getSem();
        if (sem instanceof FunctionSemanticAnalyzer) {
            //TODO
            if (!(ast.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null))
                throw new SemanticException("Unsupported create-function syntax, please use create-temporary-function syntax.");
            if (verifyUDF != null) {
                String className = BaseSemanticAnalyzer.unescapeSQLString(ast.getChild(1).getText());
                if (verifyUDF != null && verifyUDF.contains(className)) {
                    Obj obj = new Obj(className, "func", (byte) 1);
                    authSet.add(obj);
                }
            }
            validate(context, authSet, hiidoUser);

        } else if (sem instanceof DDLSemanticAnalyzer) {
            if (op == HiveOperation.DROPDATABASE) {
                Obj obj_company = new Obj(ss.getCompanyName(), "company", (byte) 15);
                authSet.add(obj_company);

            } else if (op == HiveOperation.CREATEDATABASE) {
                Obj obj = new Obj(ss.getCompanyName(), "company", (byte) 1);
                authSet.add(obj);
            } else {
                Set<WriteEntity> writeSet = context.getOutputs();
                for (WriteEntity entity : writeSet) {
                    if (entity.getType() == Entity.Type.TABLE) {
                        Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte) 1);
                        authSet.add(obj);
                    }
                }
            }
            validate(context, authSet, hiidoUser);
        } else {
            ColumnAccessInfo columnAccess = sem.getColumnAccessInfo();
            Set<WriteEntity> writeSet = context.getOutputs();
            Set<ReadEntity> readSet = context.getInputs();
            for (WriteEntity entity : writeSet) {
                if (entity.getType() == Entity.Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");
                if (entity.getType() == Entity.Type.TABLE) {
                    if ((op == HiveOperation.CREATETABLE || op == HiveOperation.CREATETABLE_AS_SELECT)) {
                        Obj obj = new Obj(entity.getName().split("@")[0], "hive", (byte) 1);
                        authSet.add(obj);
                    } else {
                        Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte) 2);
                        authSet.add(obj);
                    }
                }
                if (entity.getWriteType() == WriteEntity.WriteType.PATH_WRITE) {
                    if (entity.getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR)))
                        continue;
                    else if (entity.getType() == Entity.Type.DFS_DIR) {
                        if (entity.getName().startsWith(ss.getCompanyHdfs()))
                            continue;
                        else {
                            Obj obj = new Obj(entity.getName(), "hdfs", (byte) 2);
                            authSet.add(obj);
                        }

                    }
                    Obj obj = new Obj(entity.getD().toUri().getPath(), "hdfs", (byte) 2);
                    authSet.add(obj);
                }
            }
            for (ReadEntity entity : readSet) {
                if (entity.getType() == Entity.Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");

                else if (entity.getType() == Entity.Type.TABLE && entity.isDirect()) {
                    Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte) 4);
                    authSet.add(obj);
                }
            }
            validate(context, authSet, hiidoUser);
            sendHqltrace(hiidoUser.uid, context.getUserName(), authSet);
        }
    }

    private void sendHqltrace(int uid, String bususer, Set<Obj> authSet) {
        if(authSet == null || authSet.size() ==0)
            return;
        HttpClient client = new HttpClient();
        HttpConnectionManagerParams params = new HttpConnectionManagerParams();
        params.setSoTimeout(10 * 1000);
        params.setConnectionTimeout(10 * 1000);
        HttpConnectionManager manager = new SimpleHttpConnectionManager();
        manager.setParams(params);
        client.setHttpConnectionManager(manager);

        Protocol myhttps =  new Protocol("https", new EasySSLProtocolSocketFactory(), 443);
        client.getHostConfiguration().setHost("cloud.hiido.com", 443, myhttps);

        PostMethod method = new PostMethod("/api/hqltrace");

        StringBuilder tblBuilder = new StringBuilder();
        StringBuilder colBuilder = new StringBuilder();

        for(Obj obj : authSet) {
            if(obj.type.equals("hive") && obj.name.contains(".")){
                String table = obj.name;
                tblBuilder.append(table).append(";");
            }
        }
        List<NameValuePair> nvps = new ArrayList <NameValuePair>();
        nvps.add(new NameValuePair("uid", String.valueOf(uid)));
        nvps.add(new NameValuePair("bususer", bususer));
        nvps.add(new NameValuePair("dbtbname", tblBuilder.toString()));
        nvps.add(new NameValuePair("fields", colBuilder.toString()));
        nvps.add(new NameValuePair("qid", conf.get("hcat.qid")));
        method.setRequestBody(nvps.toArray(new NameValuePair[5]));
        try {
            client.executeMethod(method);
            LOG.debug(String.format("%s, %s", tblBuilder.toString(), colBuilder.toString()));
        } catch (Exception e) {
            LOG.debug("failed to request hqltrace server.", e);
        } finally {
            method.releaseConnection();
        }
    }

    private void validate(HiveSemanticAnalyzerHookContext context, Set<Obj> authSet, HiidoUser hiidoUser){
        if (authSet.size() == 0)
            return;

        String[] servers = HiveConfConstants.getHcatHvaserver(conf).split(";");
        String errorMessage = null;
        for(String server : servers) {
            try(CloseableHttpClient httpclient = HttpClients.custom().build();
                THttpClient thc = new THttpClient(server, httpclient)) {
                TProtocol lopFactory = new TBinaryProtocol(thc);
                HvaService.Client hvaClient =new HvaService.Client(lopFactory);
                Reply reply = null;
                if(hiidoUser.uid != 0)
                    reply = hvaClient.validate("hcat", hiidoUser, authSet);
                else
                    reply = hvaClient.validate4Dw("hcat", SessionState.get().getCurrUser(), authSet);

                if (reply.getRecode() != Recode.SUCESS)
                    if(hiidoUser.comparyId == 189 && context.getUserName() != HcatConstantConf.NULL_BUSUSER) {
                        LOG.warn("new validation message : " + reply.message);
                        errorMessage = reply.message;
                        break;
                    } else
                        throw new AuthorizationException(reply.getMessage());
                else
                    return;
            } catch (RuntimeException | TTransportException e) {
                continue;
            } catch (Exception e) {
                if(hiidoUser.comparyId != 189 || context.getUserName() == HcatConstantConf.NULL_BUSUSER)
                    throw new AuthorizationException(String.format("failed to connect authorization Server : %s", errorMessage == null?e.getMessage() : errorMessage));
                else
                    LOG.error("failed to connect authorization Server.", e);
            }
        }
    }

    private void addColumnInfo(SecurityAuth.AuthEntry.ObjectInfo objectInfo, ColumnAccessInfo columnAccess, String tableName) {
        if (columnAccess != null) {
            List<String> columns = columnAccess.getTableToColumnAccessMap().get(tableName);
            if (columns != null && columns.size() > 0)
                for (String s : columns)
                    objectInfo.addExtra(s);
            else
                objectInfo.addExtra("*");
        }
    }

    public static boolean filterHiveOperation(HiveOperation op) {
        switch (op) {
            case SWITCHDATABASE:
                return false;
            //case EXPLAIN:
            case DESCFUNCTION:
            case SHOWDATABASES:
            case SHOWTABLES:
            case SHOWCOLUMNS:
            case SHOW_TABLESTATUS:
            case SHOW_TBLPROPERTIES:
            case SHOWFUNCTIONS:
            case SHOWINDEXES:
            case SHOWCONF:

            case EXPORT:
            case IMPORT:
            case LOCKDB:
            case UNLOCKDB:
            case LOCKTABLE:
            case UNLOCKTABLE:
            case CREATEROLE:
            case DROPROLE:
            case GRANT_PRIVILEGE:
            case REVOKE_PRIVILEGE:
            case SHOW_GRANT:
            case GRANT_ROLE:
            case REVOKE_ROLE:
            case SHOW_ROLES:
            case SHOW_ROLE_PRINCIPALS:
            case SHOW_ROLE_GRANT:
            case SHOW_TRANSACTIONS:
            case START_TRANSACTION:
            case COMMIT:
            case SET_AUTOCOMMIT:
            case ABORT_TRANSACTIONS:
            case ROLLBACK:
            case MSCK:
            case ANALYZE_TABLE:
            case ALTERDATABASE_OWNER:
            case ALTERTABLE_LOCATION:
            case ALTERPARTITION_LOCATION:
                throw new AuthorizationException(String.format("fail to auth: not support this operation [%s].", op.name()));
            default:
                return true;
        }
    }
}
