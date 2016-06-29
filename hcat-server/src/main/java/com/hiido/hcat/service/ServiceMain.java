package com.hiido.hcat.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException;
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
		try {
			server.start();
			Thread.currentThread().join();
		} catch(Exception	e) {
			log.error("Server stop with exception .", e);
		}
	}
}
