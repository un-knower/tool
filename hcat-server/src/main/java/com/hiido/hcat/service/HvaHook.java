package com.hiido.hcat.service;

import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.hva.thrift.protocol.*;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zrc on 16-10-8.
 */
public class HvaHook extends AbstractSemanticAnalyzerHook {

    private static final Logger LOG = Logger.getLogger(HvaHook.class);
    private HiveConf conf;
    private ASTNode ast;

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
        this.conf = (HiveConf) context.getConf();
        this.ast = ast;
        return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context, List<Task<? extends Serializable>> rootTasks)
            throws SemanticException {
        HiveOperation op = ((HiveSemanticAnalyzerHookContextImpl)context).getSem().getQueryState().getHiveOperation();
        if (!filterHiveOperation(op))
            return;

        SessionState ss = SessionState.get();
        HiidoUser hiidoUser = new HiidoUser(ss.getHiidoUserId(), ss.getHiidoCompanyId());
        Set<Obj> authSet = new HashSet<Obj>();

        BaseSemanticAnalyzer sem = ((HiveSemanticAnalyzerHookContextImpl) context).getSem();
        if (sem instanceof FunctionSemanticAnalyzer) {

        } else if (sem instanceof DDLSemanticAnalyzer) {
            if(op == HiveOperation.CREATEDATABASE || op == HiveOperation.DROPDATABASE) {
                Obj obj = new Obj(String.valueOf(hiidoUser.comparyId), "company", (byte)1);
                authSet.add(obj);
            } else {
                Set<WriteEntity> writeSet = context.getOutputs();
                for (WriteEntity entity : writeSet) {
                    if (entity.getType() == Entity.Type.TABLE) {
                        Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte)1);
                        authSet.add(obj);
                    }
                }
            }
        } else {
            Set<WriteEntity> writeSet = context.getOutputs();
            Set<ReadEntity> readSet = context.getInputs();
            for (WriteEntity entity : writeSet) {
                if (entity.getType() == Entity.Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");
                if (entity.getType() == Entity.Type.TABLE) {
                    if ((op == HiveOperation.CREATETABLE || op == HiveOperation.CREATETABLE_AS_SELECT)) {
                        Obj obj = new Obj(entity.getName().split("@")[0], "hive", (byte)1);
                        authSet.add(obj);
                    }
                    else {
                        Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte)2);
                        authSet.add(obj);
                    }

                }
                if(entity.getWriteType() == WriteEntity.WriteType.PATH_WRITE) {
                    if (entity.getName().startsWith(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR)))
                        continue;
                    Obj obj = new Obj(entity.getD().toUri().getPath(), "hdfs", (byte)2);
                    authSet.add(obj);
                }
            }
            for (ReadEntity entity : readSet) {
                if (entity.getType() == Entity.Type.LOCAL_DIR)
                    throw new AuthorizationException("hcat not support read/write local dir.");

                if (entity.getType() == Entity.Type.TABLE && entity.isDirect()) {
                    Obj obj = new Obj(entity.getName().replace('@', '.'), "hive", (byte)4);
                    authSet.add(obj);
                }
            }
        }
        if(authSet.size() == 0)
            return;
        //TODO
        TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters();
        params.setKeyStore(HiveConfConstants.getHcatkeystorePath(conf), HiveConfConstants.getHcatKeystorePass(conf));
        params.setTrustStore(HiveConfConstants.getHcatTruststorePath(conf), HiveConfConstants.getHcatTruststorePass(conf));
        String[] hvaServer = HiveConfConstants.getHcatHvaserver(conf).split(":");
        try(TSocket socket = TSSLTransportFactory.getClientSocket(hvaServer[0], Integer.parseInt(hvaServer[1]), 5000, params)) {
            TProtocol protocol = new TBinaryProtocol(socket);
            HvaService.Client hvaClient = new HvaService.Client(protocol);
            Reply reply = hvaClient.validate("hcat", hiidoUser, authSet);
            if(reply.getRecode() != Recode.SUCESS)
                throw new AuthorizationException("fail to auth:" + reply.getMessage() == null ? "NULL" : reply.getMessage());
        } catch(TException e) {
            LOG.error("failed to connect authorization Server.", e);
            throw new AuthorizationException(String.format("failed to connect authorization Server : %s", e.getMessage()));
        }
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
            default:
                return true;
        }
    }
}
