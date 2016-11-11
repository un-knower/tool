package com.hiido.hcat.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ServiceMain {
	public static final String LOG4J = "hcat-log4j.properties";
	public static String SERVER_BEAN_ID = "hcatserver";
	
	private static final Logger log = Logger.getLogger(ServiceMain.class);
	static {
		Configuration.addDefaultResource("hive-site.xml");
        try {
			LogManager.resetConfiguration();
			PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource(LOG4J));
			LogUtils.initHiveExecLog4j();
		} catch (LogInitializationException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		ApplicationContext ctx = null;
		if(args != null && args.length==1)
			ctx = new ClassPathXmlApplicationContext(args[0]);
		else
			ctx = new ClassPathXmlApplicationContext("hcatServer.xml");
		HttpHiveServer server = ctx.getBean(SERVER_BEAN_ID, HttpHiveServer.class);

		try {
			server.start();
			log.info("HttpHiveServer is starting, port is " + server.getPort());
			Thread.currentThread().join();
		} catch(Exception e) {
			log.error("Server stop with exception .", e);
		} finally{
			log.info("HttpHiveServer is closed now.");
		}
	}
}
