package com.hiido.hcat.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.FunctionSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.hiido.hcat.common.PublicConstant;
import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.suit.net.http.protocol.SecurityAuth;
import com.hiido.suit.net.http.protocol.SecurityAuth.AuthEntry;
import com.hiido.suit.service.security.SecurityObject;
import com.hiido.suit.service.security.SecurityObject.PriviType;

public class HiveValidationHook extends AbstractSemanticAnalyzerHook {

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
		} else {
			List<AuthEntry> authInfo = new LinkedList<AuthEntry>();
			Set<WriteEntity> writeSet = context.getOutputs();
			Set<ReadEntity> readSet = context.getInputs();
			
			for(WriteEntity entity : writeSet) {
				if(entity.getType() == Type.LOCAL_DIR)
					throw new AuthorizationException("hcat not support read/write local dir.");
				if(entity.getType() == Type.TABLE)
					System.out.println(String.format("write entity %s: %s", entity.getTyp().name(), entity.getName()));
			}
			for(ReadEntity entity : readSet) {
				if(entity.getType() == Type.LOCAL_DIR)
					throw new AuthorizationException("hcat not support read/write local dir.");
				System.out.println(String.format("read entity %s: %s", entity.getTyp().name(), entity.getName()));
			}
			
			ColumnAccessInfo columnAccess = sem.getColumnAccessInfo();
			if(columnAccess != null) {
				Map<String, List<String>> map = columnAccess.getTableToColumnAccessMap();
				for(String s : map.keySet()) {
					StringBuilder builder = new StringBuilder();
					for(String ss : map.get(s))
						builder.append(ss).append(",");
					System.out.println(String.format("column access: %s, %s", s, builder.toString()));
				}
			}
		}
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
		default:
				return true;
		}
	}
}
