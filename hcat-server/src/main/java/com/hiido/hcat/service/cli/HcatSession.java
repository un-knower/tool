package com.hiido.hcat.service.cli;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.ExecuteStatementOperation;
import org.apache.hive.service.cli.operation.GetColumnsOperation;
import org.apache.hive.service.cli.operation.GetFunctionsOperation;
import org.apache.hive.service.cli.operation.GetTableTypesOperation;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.hcat.common.util.ErrCacheOutputStream;

public class HcatSession implements HiveSession {

	private static final Logger LOG = LoggerFactory.getLogger(HcatSession.class);

	private OperationManager operationManager;
	private SessionState sessionState;
	private final long creationTime;
	private final HiveConf hiveConf;
	//private Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();
	
	private Stack<OperationHandle> opHandleStack = new Stack<OperationHandle>();
	
	private volatile boolean cancel = false;
	private Hive sessionHive;

	private String username;
	private String password;
	private String ipAddress;
	
	private ErrCacheOutputStream err = null;

	public HcatSession(String username, String password, HiveConf serverhiveConf, String ipAddress) {
		this.username = username;
		this.password = password;
		this.creationTime = System.currentTimeMillis();
		this.hiveConf = new HiveConf(serverhiveConf);
		this.ipAddress = ipAddress;
	}

	private OperationManager getOperationManager() {
		return operationManager;
	}
	
	public ErrCacheOutputStream getErr() {
		return err;
	}
	
	public void setOperationManager(OperationManager operationManager) {
		this.operationManager = operationManager;
	}

	@Override
	public TProtocolVersion getProtocolVersion() {
		return TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8;
	}

	@Override
	public void setSessionManager(SessionManager sessionManager) {
		throw new UnsupportedOperationException("HcatSession not support SessionMeanager.");
	}

	@Override
	public SessionManager getSessionManager() {
		return null;
	}

	@Override
	public boolean isOperationLogEnabled() {
		return false;
	}

	@Override
	public File getOperationLogSessionDir() {
		return null;
	}

	@Override
	public void setOperationLogSessionDir(File operationLogRootDir) {
		throw new UnsupportedOperationException("hcat not support sessionDir.");
	}

	@Override
	public SessionHandle getSessionHandle() {
		return null;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public HiveConf getHiveConf() {
		return hiveConf;
	}

	@Override
	public SessionState getSessionState() {
		return sessionState;
	}

	@Override
	public String getUserName() {
		return username;
	}

	@Override
	public void setUserName(String userName) {
		username = userName;
	}

	@Override
	public String getIpAddress() {
		return ipAddress;
	}

	@Override
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}

	@Override
	public long getLastAccessTime() {
		return 0;
	}

	@Override
	public long getCreationTime() {
		return creationTime;
	}

	@Override
	public int getOpenOperationCount() {
		return opHandleStack.size();
	}
	
	public OperationHandle getCurrentOperationHandle() {
		OperationHandle handle = null;
		try {
			handle = opHandleStack.peek();
		} catch(EmptyStackException e) {
		}
		return handle;
	}

	//FIXME
	public void openWithoutStartSs(Map<String, String> sessionConfMap) throws Exception {
		sessionState = new SessionState(hiveConf, username);
		sessionState.setUserIpAddress(ipAddress);
		// sessionState.setIsHiveServerQuery(true);
		err = new ErrCacheOutputStream();
		try {
			sessionHive = Hive.get(hiveConf);
		} catch (HiveException e) {
			throw new HiveSQLException("Failed to get metastore connection", e);
		}
	}

	@Override
	public void open(Map<String, String> sessionConfMap) throws Exception {
		sessionState = new SessionState(hiveConf, username);
		sessionState.setUserIpAddress(ipAddress);
		// sessionState.setIsHiveServerQuery(true);
		err = new ErrCacheOutputStream();
		SessionState.start(sessionState);
		try {
			sessionState.reloadAuxJars();
		} catch (IOException e) {
			String msg = "Failed to load reloadable jar file path: " + e;
			LOG.error(msg, e);
			throw new HiveSQLException(msg, e);
		}
		try {
			sessionHive = Hive.get(hiveConf);
		} catch (HiveException e) {
			throw new HiveSQLException("Failed to get metastore connection", e);
		}
	}

	@Override
	public IMetaStoreClient getMetaStoreClient() throws HiveSQLException {
		try {
			return getSessionHive().getMSC();
		} catch (MetaException e) {
			throw new HiveSQLException("Failed to get metastore connection: " + e, e);
		}
	}

	@Override
	public Hive getSessionHive() throws HiveSQLException {
		return sessionHive;
	}

	@Override
	public GetInfoValue getInfo(GetInfoType getInfoType) throws HiveSQLException {
		switch (getInfoType) {
		case CLI_SERVER_NAME:
			return new GetInfoValue("Hive");
		case CLI_DBMS_NAME:
			return new GetInfoValue("Apache Hive");
		case CLI_DBMS_VER:
			return new GetInfoValue(HiveVersionInfo.getVersion());
		case CLI_MAX_COLUMN_NAME_LEN:
			return new GetInfoValue(128);
		case CLI_MAX_SCHEMA_NAME_LEN:
			return new GetInfoValue(128);
		case CLI_MAX_TABLE_NAME_LEN:
			return new GetInfoValue(128);
		case CLI_TXN_CAPABLE:
		default:
			throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString());
		}
	}

	@Override
	public OperationHandle executeStatement(String statement, Map<String, String> confOverlay) throws HiveSQLException {
		return executeStatementInternal(statement, confOverlay, false);
	}

	@Override
	public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay)
			throws HiveSQLException {
		throw new UnsupportedOperationException("Hcat not support run task async.");
	}

	@Override
	public OperationHandle getTypeInfo() throws HiveSQLException {
		return null;
	}

	@Override
	public OperationHandle getCatalogs() throws HiveSQLException {
		// do nothing
		return null;
	}

	@Override
	public OperationHandle getSchemas(String catalogName, String schemaName) throws HiveSQLException {
		// do nothing
		return null;
	}

	@Override
	public OperationHandle getTables(String catalogName, String schemaName, String tableName, List<String> tableTypes)
			throws HiveSQLException {
		// do nothing
		return null;
	}

	@Override
	public OperationHandle getTableTypes() throws HiveSQLException {
		OperationManager operationManager = getOperationManager();
		GetTableTypesOperation operation = operationManager.newGetTableTypesOperation(this);
		OperationHandle opHandle = operation.getHandle();
		try {
			operation.run();
			addOpHandle(opHandle);
			return opHandle;
		} catch (HiveSQLException e) {
			operationManager.closeOperation(opHandle);
			throw e;
		}
	}

	@Override
	public OperationHandle getColumns(String catalogName, String schemaName, String tableName, String columnName)
			throws HiveSQLException {
		String addedJars = Utilities.getResourceFiles(hiveConf, SessionState.ResourceType.JAR);
		if (StringUtils.isNotBlank(addedJars)) {
			IMetaStoreClient metastoreClient = getMetaStoreClient();
			metastoreClient.setHiveAddedJars(addedJars);
		}
		OperationManager operationManager = getOperationManager();
		GetColumnsOperation operation = operationManager.newGetColumnsOperation(this, catalogName, schemaName,
				tableName, columnName);
		OperationHandle opHandle = operation.getHandle();
		try {
			operation.run();
			addOpHandle(opHandle);
			return opHandle;
		} catch (HiveSQLException e) {
			operationManager.closeOperation(opHandle);
			throw e;
		}
	}

	@Override
	public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
			throws HiveSQLException {

		OperationManager operationManager = getOperationManager();
		GetFunctionsOperation operation = operationManager.newGetFunctionsOperation(this, catalogName, schemaName,
				functionName);
		OperationHandle opHandle = operation.getHandle();
		try {
			operation.run();
			addOpHandle(opHandle);
			return opHandle;
		} catch (HiveSQLException e) {
			operationManager.closeOperation(opHandle);
			throw e;
		}
	}

	@Override
	public void close() throws HiveSQLException {
		try {
			List<OperationHandle> ops = null;
			synchronized (opHandleStack) {
				ops = new ArrayList<>(opHandleStack);
				opHandleStack.clear();
			}
			for (OperationHandle opHandle : ops) {
				operationManager.closeOperation(opHandle);
			}
			HiveHistory hiveHist = sessionState.getHiveHistory();
			if (null != hiveHist) {
				hiveHist.closeStream();
			}
			try {
				sessionState.close();
			} finally {
				sessionState = null;
			}
		} catch (IOException ioe) {
			throw new HiveSQLException("Failure to close", ioe);
		} finally {
			if (sessionState != null) {
				try {
					sessionState.close();
				} catch (Throwable t) {
					LOG.warn("Error closing session", t);
				}
				sessionState = null;
			}
			if (sessionHive != null) {
				try {
					Hive.closeCurrent();
				} catch (Throwable t) {
					LOG.warn("Error closing sessionHive", t);
				}
				sessionHive = null;
			}
		}
	}
	
	public void cancel() throws HiveSQLException {
		OperationHandle handle = null;
		synchronized(opHandleStack) {
			this.cancel = true;
			handle = this.opHandleStack.peek();
		}
		Operation opt = operationManager.getOperation(handle);
		if(!(opt.isFailed() || opt.isCanceled() || opt.isFinished()))
			opt.cancel();
	}

	@Override
	public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
		operationManager.cancelOperation(opHandle);
	}

	@Override
	public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
		operationManager.closeOperation(opHandle);
	}

	@Override
	public TableSchema getResultSetMetadata(OperationHandle opHandle) throws HiveSQLException {
		return operationManager.getOperationResultSetSchema(opHandle);
	}

	@Override
	public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows,
			FetchType fetchType) throws HiveSQLException {
		// do nothing
		return null;
	}

	@Override
	public String getDelegationToken(HiveAuthFactory authFactory, String owner, String renewer)
			throws HiveSQLException {
		return null;
	}

	@Override
	public void cancelDelegationToken(HiveAuthFactory authFactory, String tokenStr) throws HiveSQLException {
		// do nothing
	}

	@Override
	public void renewDelegationToken(HiveAuthFactory authFactory, String tokenStr) throws HiveSQLException {
		// do nothing
	}

	@Override
	public void closeExpiredOperations() {
		// do nothing
	}

	@Override
	public long getNoOperationTime() {
		return 0;
	}

	private void addOpHandle(OperationHandle opHandle) {
		synchronized (opHandleStack) {
			opHandleStack.add(opHandle);
		}
	}
	
	private OperationHandle pop() {
		synchronized (opHandleStack) {
			try {
				return opHandleStack.pop();
			} catch(EmptyStackException e) {
				return null;
			}
		}
	}

	private OperationHandle executeStatementInternal(String statement, Map<String, String> confOverlay,
			boolean runAsync) throws HiveSQLException {
		if(runAsync)
			throw new UnsupportedOperationException("not support async");
		ExecuteStatementOperation operation = operationManager
		        .newExecuteStatementOperation(this, statement, confOverlay, runAsync);
		OperationHandle opHandle = operation.getHandle();
		synchronized(this.opHandleStack) {
			if(cancel) {
				operationManager.cancelOperation(opHandle);
				return null;
			}
			addOpHandle(opHandle);
		}
		try{
			operation.run();
			return opHandle;
		} catch (HiveSQLException e) {
		      // Refering to SQLOperation.java,there is no chance that a HiveSQLException throws and the asyn
		      // background operation submits to thread pool successfully at the same time. So, Cleanup
		      // opHandle directly when got HiveSQLException
		      operationManager.closeOperation(opHandle);
		      pop();
		      throw e;
		    }
	}
}
