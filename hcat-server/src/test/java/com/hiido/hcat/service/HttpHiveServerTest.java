package com.hiido.hcat.service;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class HttpHiveServerTest {

	public void test() {
		fail("Not yet implemented");
	}
	
	public static void main(String[] args) {
		System.out.println(System.currentTimeMillis());
		Configuration.addDefaultResource("hive-site.xml");
		HttpHiveServer  server = new HttpHiveServer("eth0", 26018);
		try {
			server.start();
			Thread.currentThread().join();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
