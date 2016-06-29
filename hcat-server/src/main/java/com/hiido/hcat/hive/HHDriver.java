package com.hiido.hcat.hive;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.hooks.Hook;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContextImpl;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hcat Hive Driver
 * 
 * @author zrc
 *
 */
public class HHDriver implements CommandProcessor {
	static final private Logger LOG = LoggerFactory.getLogger(HHDriver.class);
	static final private String CLASS_NAME = Driver.class.getName();
	static final private LogHelper console = new LogHelper(LOG);

	private HiveConf conf;
	private String userName;

	// A list of FileSinkOperators writing in an ACID compliant manner
	private Set<FileSinkDesc> acidSinks;

	public HHDriver(HiveConf conf) {
		this.conf = conf;
	}

	public void init() {
		// TODO Auto-generated method stub

	}

	public int compile(String command) {
		return compile(command, true);
	}

	public int compile(String command, boolean resetTaskIds) {
		PerfLogger perfLogger = SessionState.getPerfLogger();
		perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.COMPILE);

		command = new VariableSubstitution(new HiveVariableSource() {
			@Override
			public Map<String, String> getHiveVariable() {
				return SessionState.get().getHiveVariables();
			}
		}).substitute(conf, command);

		String queryStr = command;

		try {
			// command should be redacted to avoid to logging sensitive data
			queryStr = HookUtils.redactLogString(conf, command);
		} catch (Exception e) {
			LOG.warn("WARNING! Query command could not be redacted." + e);
		}

		String queryId = QueryPlan.makeQueryId();
		try {
			Context ctx = new Context(conf);
			ctx.setTryCount(0);
			ctx.setCmd(queryStr);
			ctx.setHDFSCleanup(true);
			ParseDriver pd = new ParseDriver();
			ASTNode tree = pd.parse(command, ctx);
			tree = ParseUtils.findRootNonNullToken(tree);

			BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);
			HiveOperation operation = SessionState.get().getHiveOperation();
			
			List<HiveSemanticAnalyzerHook> saHooks = getHooks(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
					HiveSemanticAnalyzerHook.class);

			// Flush the metastore cache. This assures that we don't pick up
			// objects from a previous
			// query running in this same thread. This has to be done after we
			// get our semantic
			// analyzer (this is when the connection to the metastore is made)
			// but before we analyze,
			// because at that point we need access to the objects.
			Hive.get().getMSC().flushCache();

			// Do semantic analysis and plan generation
			if (saHooks != null && !saHooks.isEmpty()) {
				HiveSemanticAnalyzerHookContext hookCtx = new HiveSemanticAnalyzerHookContextImpl();
				hookCtx.setConf(conf);
				hookCtx.setUserName(userName);
				hookCtx.setIpAddress(SessionState.get().getUserIpAddress());
				hookCtx.setCommand(command);
				for (HiveSemanticAnalyzerHook hook : saHooks) {
					tree = hook.preAnalyze(hookCtx, tree);
				}
			} else
				sem.analyze(tree, ctx);

			// Record any ACID compliant FileSinkOperators we saw so we can add
			// our transaction ID to
			// them later.
			acidSinks = sem.getAcidFileSinks();
			LOG.info("Semantic Analysis Completed");

			sem.validate();
			Schema schema = getSchema(sem, conf);

			QueryPlan plan = new QueryPlan(queryStr, sem, perfLogger.getStartTime(PerfLogger.DRIVER_RUN), queryId,
					SessionState.get().getHiveOperation(), schema);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SemanticException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}

		return 0;
	}

	private static Schema getSchema(BaseSemanticAnalyzer sem, HiveConf conf) {

		Schema schema = null;

		// If we have a plan, prefer its logical result schema if it's
		// available; otherwise, try digging out a fetch task; failing that,
		// give up.
		if (sem == null) {
			// can't get any info without a plan
		} else if (sem.getResultSchema() != null) {
			List<FieldSchema> lst = sem.getResultSchema();
			schema = new Schema(lst, null);
		} else if (sem.getFetchTask() != null) {
			FetchTask ft = sem.getFetchTask();
			TableDesc td = ft.getTblDesc();
			// partitioned tables don't have tableDesc set on the FetchTask.
			// Instead
			// they have a list of PartitionDesc objects, each with a table
			// desc.
			// Let's
			// try to fetch the desc for the first partition and use it's
			// deserializer.
			if (td == null && ft.getWork() != null && ft.getWork().getPartDesc() != null) {
				if (ft.getWork().getPartDesc().size() > 0) {
					td = ft.getWork().getPartDesc().get(0).getTableDesc();
				}
			}

			if (td == null) {
				LOG.info("No returning schema.");
			} else {
				String tableName = "result";
				List<FieldSchema> lst = null;
				try {
					lst = MetaStoreUtils.getFieldsFromDeserializer(tableName, td.getDeserializer(conf));
				} catch (Exception e) {
					LOG.warn("Error getting schema: " + org.apache.hadoop.util.StringUtils.stringifyException(e));
				}
				if (lst != null) {
					schema = new Schema(lst, null);
				}
			}
		}
		if (schema == null) {
			schema = new Schema();
		}
		LOG.info("Returning Hive schema: " + schema);
		return schema;

	}

	/**
	 * Returns the hooks specified in a configuration variable.
	 *
	 * @param hookConfVar
	 *            The configuration variable specifying a comma separated list
	 *            of the hook class names.
	 * @param clazz
	 *            The super type of the hooks.
	 * @return A list of the hooks cast as the type specified in clazz, in the
	 *         order they are listed in the value of hookConfVar
	 * @throws Exception
	 */
	private <T extends Hook> List<T> getHooks(ConfVars hookConfVar, Class<T> clazz) throws Exception {
		try {
			return HookUtils.getHooks(conf, hookConfVar, clazz);
		} catch (ClassNotFoundException e) {
			console.printError(hookConfVar.varname + " Class not found:" + e.getMessage());
			throw e;
		}
	}

	public CommandProcessorResponse run(String command) throws CommandNeedRetryException {
		// TODO Auto-generated method stub
		return null;
	}

}
