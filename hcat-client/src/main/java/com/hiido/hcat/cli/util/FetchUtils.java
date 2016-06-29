package com.hiido.hcat.cli.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;

public class FetchUtils {

	public static String[] limitFetch(String serializedStr, int limit) {
		List<String> list = new LinkedList<String>();
		try {
			FetchTask fetch = SerializationUtilities.deserializeObject(serializedStr, FetchTask.class);
			Configuration c = new Configuration(false);
			fetch.initialize(new HiveConf(c, FetchUtils.class), null, null, new CompilationOpContext());

			fetch.setMaxRows(limit);
			fetch.fetch(list);
		} catch (IOException | CommandNeedRetryException e) {
			
		}
		return list.toArray(new String[list.size()]);
	}
}
