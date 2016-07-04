package com.hiido.hcat.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.hiido.hcat.common.PublicConstant;
import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.suit.net.http.protocol.SecurityAuth;
import com.hiido.suit.net.http.protocol.SecurityAuth.AuthEntry;
import com.hiido.suit.service.security.SecurityObject;
import com.hiido.suit.service.security.SecurityObject.PriviType;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.plans.logical.Except;

public class HiveValidationHook extends AbstractSemanticAnalyzerHook {
	private static final Logger LOG = Logger.getLogger(HiveValidationHook.class);
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
		
		HiveOperation op = SessionState.get().getHiveOperation();
		if(!filterHiveOperation(op))
			return;
		
		BaseSemanticAnalyzer sem = ((HiveSemanticAnalyzerHookContextImpl) context).getSem();
		List<AuthEntry> authInfo = new LinkedList<AuthEntry>();
		if(sem instanceof FunctionSemanticAnalyzer) {
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
		} else if(sem instanceof DDLSemanticAnalyzer) {
			if(op == HiveOperation.DROPTABLE || op == HiveOperation.DROPVIEW) {
				Set<WriteEntity> writeSet = context.getOutputs();

				for(WriteEntity entity : writeSet) {
					if(entity.getType() == Type.TABLE) {
						AuthEntry entry = new AuthEntry();
						entry.setPrivi_type(PriviType.DROPTABLE.toString());
						entry.setBusi_type(Business.BusType.HIVE.toString());
						AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
						String object_name = String.format("%s.%s", "default", entity.getName());
						System.out.println("drop auth:" + object_name);

						objectInfo.setObject_name(object_name);
						entry.setObject_info(objectInfo);
						authInfo.add(entry);
					}
				}
			}
		} else {
			Set<WriteEntity> writeSet = context.getOutputs();
			Set<ReadEntity> readSet = context.getInputs();
			
			for(WriteEntity entity : writeSet) {
				if(entity.getType() == Type.LOCAL_DIR)
					throw new AuthorizationException("hcat not support read/write local dir.");
				if(entity.getType() == Type.TABLE ) {
					AuthEntry entry = new AuthEntry();
					if((op != HiveOperation.CREATETABLE || op != HiveOperation.CREATETABLE_AS_SELECT ))
						entry.setPrivi_type(PriviType.CREATETABLE.toString());
					else
						entry.setPrivi_type(PriviType.WRITETABLE.toString());
					entry.setBusi_type(Business.BusType.HIVE.toString());
					AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
					String object_name = String.format("%s.%s", "default", entity.getName());
					objectInfo.setObject_name(object_name);
					entry.setObject_info(objectInfo);
					authInfo.add(entry);
					LOG.debug(String.format("write entity %s: %s", entity.getTyp().name(), entity.getName()));
				}
			}
			for(ReadEntity entity : readSet) {
				if(entity.getType() == Type.LOCAL_DIR)
					throw new AuthorizationException("hcat not support read/write local dir.");

				if(entity.getType() == Type.TABLE) {
					AuthEntry entry = new AuthEntry();
					entry.setPrivi_type(PriviType.QUERY.toString());
					entry.setBusi_type(Business.BusType.HIVE.toString());
					AuthEntry.ObjectInfo objectInfo = new AuthEntry.ObjectInfo();
					String object_name = String.format("%s.%s", "default", entity.getName());
					objectInfo.setObject_name(object_name);
					entry.setObject_info(objectInfo);
					authInfo.add(entry);
					LOG.debug(String.format("read entity %s: %s", entity.getTyp().name(), entity.getName()));
				}
			}
			
			ColumnAccessInfo columnAccess = sem.getColumnAccessInfo();
			if(columnAccess != null) {
				Map<String, List<String>> map = columnAccess.getTableToColumnAccessMap();
				for(String s : map.keySet()) {
					StringBuilder builder = new StringBuilder();
					for(String ss : map.get(s))
						builder.append(ss).append(",");
					LOG.debug(String.format("column access: %s, %s", s, builder.toString()));
				}
			}
			//objectInfo.addExtra("col1");
			//objectInfo.addExtra("col2");
		}
		SecurityAuth sa = createSecurityAuth();
		com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient client = new com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient();
		try {
			client.setAddrList(conf.get(PublicConstant.HCAT_AUTHENTICATION_SERVERS));
			client.setClientNum(-1);
			com.hiido.suit.net.http.protocol.HttpApacheClient apacheClient = new com.hiido.suit.net.http.protocol.HttpApacheClient();
			client.setHttpProtocolClient(apacheClient);
			SecurityAuth.Reply reply = null;

			reply = client.post(sa, SecurityAuth.Reply.class);

			if (reply == null || !"success".equals(reply.getResp_code())) {
				throw new AuthorizationException("fail to auth:" + reply == null ? "NULL" : reply.toString());
			}
		}catch(Exception e) {
			if(e instanceof AuthorizationException)
				throw (AuthorizationException)e;
			LOG.error("failed to connect authorization Server.", e);
			throw new AuthorizationException(String.format("failed to connect authorization Server : %s", e.getMessage()));
		} finally {
			client.close();
		}
	}


	protected SecurityAuth createSecurityAuth() {
        SecurityAuth sa = new SecurityAuth();
		sa.setClient_user_name("superman");
		sa.setClient_passwd("032ce83b465499938dhg77d8bc9ef7fc");
		sa.setVersion("1.02");
        sa.setRequest_type(SecurityAuth.REQUEST_TYPE_AUTH);
        //sa.setUser_name(scWrapper.busUser);
        //sa.setLog_user(scWrapper.sysUser);
        //sa.setCur_user(scWrapper.curUser);
        //sa.setQuery_id(scWrapper.query_id);
        //sa.setSid(String.valueOf(scWrapper.sid));
        //sa.setQstring(scWrapper.qstring);
        return sa;
    }

	public static boolean filterHiveOperation(HiveOperation op) {
		switch(op) {
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
