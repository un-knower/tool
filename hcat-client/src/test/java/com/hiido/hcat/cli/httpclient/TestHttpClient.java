package com.hiido.hcat.cli.httpclient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.junit.Test;

import com.hiido.hcat.thrift.protocol.AuthorizationException;
import com.hiido.hcat.thrift.protocol.CancelQuery;
import com.hiido.hcat.thrift.protocol.CancelQueryReply;
import com.hiido.hcat.thrift.protocol.CliService;
import com.hiido.hcat.thrift.protocol.CommitQuery;
import com.hiido.hcat.thrift.protocol.CommitQueryReply;
import com.hiido.hcat.thrift.protocol.JobStatus;
import com.hiido.hcat.thrift.protocol.LoadFile;
import com.hiido.hcat.thrift.protocol.LoadFileReply;
import com.hiido.hcat.thrift.protocol.QueryStatus;
import com.hiido.hcat.thrift.protocol.QueryStatusReply;

import static org.junit.Assert.*;

public class TestHttpClient {

	static String serveltUrl = "http://14.17.109.51:26021/query";

	public void cancelJob() {

		try {
			THttpClient thc = new THttpClient(serveltUrl);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			CliService.Client client = new CliService.Client(lopFactory);
			CancelQueryReply reply = client
					.cancelJob(new CancelQuery().setQueryId("abc").setCipher(new HashMap<String, String>()));
			//assertEquals(recode.SUCESS, reply.code);
		} catch (TException ex) {
			ex.printStackTrace();
		}
	}

	//@Test
	public void loadFile() {
		try {
			THttpClient thc = new THttpClient(serveltUrl);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			CliService.Client client = new CliService.Client(lopFactory);
			LoadFileReply reply = client.laodData(
					new LoadFile().setQuery("").setCurrentDatabase("default").setCipher(new HashMap<String, String>()));
			//assertEquals(recode.SUCESS, reply.code);
		} catch (com.hiido.hcat.thrift.protocol.RuntimeException | AuthorizationException e) {
			if (e instanceof com.hiido.hcat.thrift.protocol.RuntimeException)
				System.out.println(((com.hiido.hcat.thrift.protocol.RuntimeException) e).getMsg());
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	public QueryStatusReply queryJobStatus(CliService.Client client, String qid) throws TException {
		return client.queryJobStatus(new QueryStatus().setQueryId(qid));
	}

	@Test
	public void commit() throws InterruptedException {
		try {
			THttpClient thc = new THttpClient(serveltUrl);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			CliService.Client client = new CliService.Client(lopFactory);
			//String sql = "use freshman; set hive.exec.mode.local.auto=true;insert overwrite local directory '/tmp/hive-zrc/abc' select school, count(*) from freshman_studs group by school;";
			//String sql = "use freshman;with q1 as (select freshman_studs.school as sc,count(*) as num from freshman.freshman_studs group by freshman_studs.school) select sc,num from q1";
			//String sql = "use freshman;set hive.cbo.enable=false;select sc, num from freshman.freshman_studs_view";
			//String sql = "use freshman;create view freshman_studs_view (sc, num) as select school,count(*) from freshman_studs group by school";
			//String sql = "use freshman create table freshman_new1(id int,name string);";
			String sql = "set hive.execution.engine=spark;select count(*) from (\n" +
					"select distinct uid, sjp from \n" +
					"yy_mbsdkdo_original where dt >=20160919 and dt<=20160920\n" +
					"  ) cc";
			HashMap<String, String> cipher = new HashMap<String, String>();
			cipher.put("loguser", "dw_zouruochen");
			cipher.put("curuser", "dw_zouruochen");
			cipher.put("bususer", "shark");
			cipher.put("skey","6VdbVqlSi2uZwPXW+TIc8MI=");
			cipher.put("username", "zouruochen");

			CommitQuery cq = new CommitQuery().setQuery(sql).setCipher(cipher);
			CommitQueryReply reply = client.commit(cq);
			if (reply.getHandle().isQuick())
				//assertEquals(reply.getStatus(), JobStatus.COMPLETE);
				System.out.println(reply.getHandle().running);
			else {
				/*
				QueryStatusReply status = queryJobStatus(client, reply.getHandle().queryId);
				while (status.queryProgress.state != JobStatus.COMPLETE.getValue() && status.queryProgress.state != JobStatus.FAILURE.getValue()
						&& status.queryProgress.state != JobStatus.CANCEL.getValue()) {
					//System.out.println(String.format("job %s", status.status.name()));
					Thread.sleep(5000);
					status = queryJobStatus(client, reply.handle.queryId);
				}
				if (status.queryProgress.state != JobStatus.COMPLETE.getValue()) {
					System.out.println("failed !");
					System.out.println(reply.handle.stderr);
					return;
				}
				*/
				/*
				for (String s : status.queryProgress.res) {
					System.out.println(s);
					FetchTask fetch = SerializationUtilities.deserializeObject(s, FetchTask.class);
					Configuration c = new Configuration(false);
					fetch.initialize(new HiveConf(c, this.getClass()), null, null, new CompilationOpContext());
					List<String> list = new LinkedList<String>();
					fetch.fetch(list);
					for (String r : list)
						System.out.println(r);
				}
				*/
			}
		} catch (com.hiido.hcat.thrift.protocol.RuntimeException | AuthorizationException e) {
			if (e instanceof com.hiido.hcat.thrift.protocol.RuntimeException)
				System.out.println(((com.hiido.hcat.thrift.protocol.RuntimeException) e).getMsg());
		} catch (TException e) {
			e.printStackTrace();
		}
	}

	//@Test
	public void fetchTask() throws IOException, CommandNeedRetryException {
		String str = "AQEAamF2YS51dGlsLkFycmF5TGlz9AEAAAAAAQFvcmcuYXBhY2hlLmhhZG9vcC5oaXZlLnFsLmV4ZWMuRmV0Y2hPcGVyYXRv8gEAAAAAAAECb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5xbC5leGVjLkxpc3RTaW5rT3BlcmF0b/IBAQNqYXZhLnV0aWwuY29uY3VycmVudC5hdG9taWMuQXRvbWljQm9vbGVh7gEAAQABAAABBG9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwucGxhbi5MaXN0U2lua0Rlc+MBAAFOVUzMAAAAAYI3AAFPUF+3AQABAAAAAAABBW9yZy5hcGFjaGUuaGFkb29wLmhpdmUucWwucGxhbi5GZXRjaFdvcusBAAEAAAkBAgUBAgUAFAEBAQZvcmcuYXBhY2hlLmhhZG9vcC5tYXByZWQuVGV4dElucHV0Rm9ybWH0AAABFwABB2phdmEudXRpbC5Qcm9wZXJ0aWXzAQYDAWNvbHVtbvMDAV9jb2wwLF9jb2yxAwFzZXJpYWxpemF0aW9uLmVzY2FwZS5jcmzmAwF0cnXlAwFzZXJpYWxpemF0aW9uLmxp4gMBb3JnLmFwYWNoZS5oYWRvb3AuaGl2ZS5zZXJkZTIubGF6eS5MYXp5U2ltcGxlU2VyROUDAWhpdmUuc2VyaWFsaXphdGlvbi5leHRlbmQuYWRkaXRpb25hbC5uZXN0aW5nLmxldmVs8wMVAwFzZXJpYWxpemF0aW9uLmZvcm1h9AMBgjEDAWNvbHVtbnMudHlwZfMDAXN0cmluZzpiaWdpbvQOAdQBaGRmczovL2xvY2FsaG9zdDo5MDAwL3RtcC9oaXZlLXpyYy8xNzJfMjVfMTlfNl8yNjAxOF8yMDE2MDYyNDE4NDgyNzYzNl8wLy1leHQtMTAwMDIBU3RhZ2UtsADIAQAAAAECBQAAAQUN";
		FetchTask fetch = SerializationUtilities.deserializeObject(str, FetchTask.class);
		Configuration c = new Configuration(false);
		fetch.initialize(new HiveConf(c, this.getClass()), null, null, new CompilationOpContext());
		List<String> list = new LinkedList<String>();
		fetch.fetch(list);
		for (String r : list)
			System.out.println(r);
	}
	
	
}
