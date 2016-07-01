package com.hiido.hcat.service;

import com.hiido.suit.common.util.JdbcConn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import com.hiido.suit.common.util.ConnectionPool;
import org.apache.log4j.Logger;

public class ServiceMain {
	
	private static final Logger log = Logger.getLogger(ServiceMain.class);
	static {
		Configuration.addDefaultResource("core-default.xml");
        Configuration.addDefaultResource("core-site.xml");
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
        Configuration.addDefaultResource("hive-site.xml");
        try {
			LogUtils.initHiveExecLog4j();
		} catch (LogInitializationException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		HttpHiveServer server = new HttpHiveServer(args[0], Integer.parseInt(args[1]));
		ConnectionPool pool = new ConnectionPool();
		JdbcConn jdbcConn = new JdbcConn();
		jdbcConn.setDriverName("com.mysql.jdbc.Driver");
		jdbcConn.setUrl("jdbc:mysql://10.23.19.33:3306/test?useUnicode=true&amp;characterEncoding=utf-8&amp;autoReconnect=true");
		jdbcConn.setUsername("hiidosys");
		jdbcConn.setPassword("01bd8bf80ef9fe0d0cbcdbb7d938548e");
		pool.setJdbcConn(jdbcConn);
		server.setConnPool(pool);
		try {
			server.start();
			log.info("HttpHiveServer is starting, port is " + args[1]);
			Thread.currentThread().join();
		} catch(Exception e) {
			log.error("Server stop with exception .", e);
		} finally{
			log.info("HttpHiveServer is closed now.");
		}
	}
}
