package com.hiido.hcat.service;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.junit.Before;
import org.junit.Test;

import com.hiido.hcat.thrift.protocol.AuthorizationException;
import com.hiido.hcat.thrift.protocol.RuntimeException;

public class QuickTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() throws AuthorizationException, RuntimeException, IOException {
		String queryStr = "set hive.fetch.task.conversion=none;\nset hive.execution.engine=spark;\nselect ip,country from default.yy_mbsdkinstall_original where dt='20160601'  and ip='122.13.229.21' ;";
		boolean quick = true;
		Configuration conf =  new Configuration();
		Context ctx = new Context(conf, false);
		ParseDriver pd = new ParseDriver();
		String command = "";
		for (String oneCmd : queryStr.split(";")) {
			if (StringUtils.endsWith(oneCmd, "\\")) {
				command += StringUtils.chop(oneCmd) + ";";
				continue;
			} else {
				command += oneCmd;
			}
			if (StringUtils.isBlank(command)) {
				continue;
			}
			quick = quick & HttpHiveServer.isQuickCmd(command, ctx, pd);
			System.out.println(quick);
		}
	}

}
