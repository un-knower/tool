package com.hiido.hcat.hive;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.hcat.service.HttpServer;

public class HcatServer extends AbstractService {

	private static final Logger LOG = LoggerFactory.getLogger(HcatServer.class);
	private HttpServer webServer;
	private final Map<String, Object> runningJobs = new HashMap<String, Object>();
	private final Map<String, Object> pendingJobs = new HashMap<String, Object>();
	
	public HcatServer(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	
	public  synchronized void init(HiveConf hiveConf) {
		
	}
	
	@Override
	public synchronized void start() {
		super.start();
		if(webServer != null) {
			try{
				webServer.start();
				LOG.info("Web UI has started on port " + webServer.getPort());
			} catch(Exception e) {
				LOG.error("Error starting Web UI: ", e);
				throw new ServiceException(e);
			}
		}
	}
}
