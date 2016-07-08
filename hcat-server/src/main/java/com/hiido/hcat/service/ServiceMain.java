package com.hiido.hcat.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ServiceMain {

	public static String SERVER_BEAN_ID = "hcatserver";
	
	private static final Logger log = Logger.getLogger(ServiceMain.class);
	static {
		Configuration.addDefaultResource("hive-site.xml");
		/*
		Configuration.addDefaultResource("core-default.xml");
        Configuration.addDefaultResource("core-site.xml");
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
		Configuration.addDefaultResource("yarn-site.xml");
		System.out.println(Thread.currentThread().getContextClassLoader().getResource("yarn-site.xml").getPath());
		*/
        try {
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
