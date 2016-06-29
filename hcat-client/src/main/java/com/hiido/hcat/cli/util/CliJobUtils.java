package com.hiido.hcat.cli.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import com.hiido.hcat.thrift.protocol.CliService;
import com.hiido.hcat.thrift.protocol.CommitQuery;
import com.hiido.hcat.thrift.protocol.NotFoundException;
import com.hiido.hcat.thrift.protocol.QueryStatus;
import com.hiido.hcat.thrift.protocol.QueryStatusReply;

public class CliJobUtils {

	public static void autoCommitJobs(String url, String path, int reCommit) {
		File parent = new File(path);
		File[] scripts = parent.listFiles();

		List<String> querys = new LinkedList<String>();
		for (File f : scripts) {
			InputStreamReader isr = null;
			StringBuilder builder = new StringBuilder();
			try {
				isr = new InputStreamReader(new FileInputStream(f), "UTF-8");
				int i = 0;
				char[] chars = new char[128];
				while ((i = isr.read(chars)) > 0) {
					for (int j = 0; j < i; j++)
						builder.append(chars[j]);
				}
				querys.add(builder.toString());
				builder.setLength(0);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if(isr != null)
					try {
						isr.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}
		}

		try {
			THttpClient thc = new THttpClient(url);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			CliService.Client client = new CliService.Client(lopFactory);
			CommitQuery cq = new CommitQuery().setCipher(Collections.<String, String> emptyMap());
			for (int i = 0; i < reCommit; i++) {
				for (String q : querys) {
					cq.setQuery(q);
					client.commit(cq);
				}
			}
		} catch (TException e) {
			e.printStackTrace();
		} finally {

		}
	}

	public static void jobStatus(String url, String jobid) {
		try {
			THttpClient thc = new THttpClient(url);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			CliService.Client client = new CliService.Client(lopFactory);
			QueryStatus request = new QueryStatus().setQueryId(jobid)
					.setCipher(Collections.<String, String> emptyMap());
			QueryStatusReply response = client.queryJobStatus(request);
			System.out.println(String.format("job status: %d, progress: %f, ", response.queryProgress.state,
					response.queryProgress.progress));
			if (response.queryProgress.fetchDirs != null) {
				System.out.println("fetch dirs are :");
				for (String p : response.queryProgress.fetchDirs)
					System.out.println(p);
			}
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (NotFoundException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args[0].equals("status"))
			jobStatus(args[1], args[2]);
		else if(args[0].equals("commit"))
			autoCommitJobs(args[1], args[2], Integer.parseInt(args[3]));
	}
}
