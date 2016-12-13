package com.hiido.hcat.cli.httpclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.hiido.hcat.thrift.protocol.*;
import com.hiido.hcat.thrift.protocol.SignupReply;
import com.hiido.hcat.thrift.protocol.SignupService;
import org.apache.commons.lang3.SystemUtils;
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
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class TestHttpClient {

    static String serveltUrl = "http://14.17.109.45:26021/query";

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

    @Test
    public void queryJobStatus() throws TException, InterruptedException {
        String qid = "14_17_109_45_26022_20161213012033542_25135";
        THttpClient thc = new THttpClient(serveltUrl);
        TProtocol lopFactory = new TBinaryProtocol(thc);
        CliService.Client client = new CliService.Client(lopFactory);
        HashMap<String, String> cipher = new HashMap<String, String>();
        cipher.put("loguser", "dw_zouruochen");
        cipher.put("curuser", "dw_zouruochen");
        cipher.put("bususer", "shark");
        cipher.put("skey", "6VdbVqlSi2uZwPXW+TIc8MI=");
        cipher.put("username", "zouruochen");
        cipher.put("user_id", "471");
        cipher.put("company_id", "189");
        QueryStatusReply status = client.queryJobStatus(new QueryStatus(cipher, qid));
        //while (status.retCode == 0 && status.getQueryProgress().state == 1) {
            for (String job : status.getQueryProgress().jobId) {
                System.out.println("job id:" + job);
            }
            System.out.println("progress: " + status.getQueryProgress().progress);
            Thread.sleep(5000);
            status = client.queryJobStatus(new QueryStatus(cipher, qid));
        //}
        if (status.retMessage != null)
            System.out.println(status.retMessage);
        System.out.println("Query is completed.");
    }

    @Test
    public void signup() {
        try {
            THttpClient thc = new THttpClient("http://14.17.109.49:26021/signup");
            TProtocol lopFactory = new TBinaryProtocol(thc);
            SignupService.Client client = new SignupService.Client(lopFactory);
            SignupReply reply = client.signup(2, "myopen", 2777);
            System.out.println(reply.getCode() + reply.getRetMessage());
        } catch (com.hiido.hcat.thrift.protocol.RuntimeException e) {
            e.printStackTrace();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void commit() throws InterruptedException {
        for (int i = 0; i < 2; i++) {
            try {
                THttpClient thc = new THttpClient(serveltUrl);
                TProtocol lopFactory = new TBinaryProtocol(thc);
                CliService.Client client = new CliService.Client(lopFactory);
                String sql = "set hive.execution.engine=spark;\n" +
                        "USE yule;\n" +
                        "create temporary function base64En as 'com.hiido.hive.udf.GenericUDFBase64En';\n" +
                        "select base64En(version),login_num\n" +
                        "from\n" +
                        "(select '7.x' as version,count(distinct uid) as login_num\n" +
                        "from default.yy_yylogin_original\n" +
                        "where dt='20161111' and ver like '7.%' and hour<='09'\n" +
                        "union all\n" +
                        "select ver as version,count(distinct uid) as login_num\n" +
                        "from default.yy_yylogin_original\n" +
                        "where dt='20161111' and (ver like '7.%' or ver like '8.%') and hour<='09'\n" +
                        "group by ver\n" +
                        ")a";
                HashMap<String, String> cipher = new HashMap<String, String>();
                cipher.put("loguser", "dw_zouruochen");
                cipher.put("curuser", "dw_zouruochen");
                cipher.put("bususer", "shark");
                cipher.put("skey", "6VdbVqlSi2uZwPXW+TIc8MI=");
                cipher.put("username", "zouruochen");
                cipher.put("user_id", "471");
                cipher.put("company_id", "189");

                CommitQuery cq = new CommitQuery().setQuery(sql).setCipher(cipher);
                CommitQueryReply reply = client.commit(cq);
                if (reply.getHandle().isQuick())
                    //assertEquals(reply.getStatus(), JobStatus.COMPLETE);
                    System.out.println(reply.getHandle().running);
                else {
                    String qid = reply.getHandle().getQueryId();
                    System.out.println(qid + " started");
                    QueryStatusReply status = client.queryJobStatus(new QueryStatus(cipher, qid));
                /*
				while(status.retCode == 0 && status.getQueryProgress().state == 1) {
					for(String job : status.getQueryProgress().jobId){
						System.out.println("job id:" + job);
					}
					System.out.println("progress: " + status.getQueryProgress().progress);
					Thread.sleep(5000);
					status = client.queryJobStatus(new QueryStatus(cipher, qid));
				}
				*/
                    if (status.retMessage != null)
                        System.out.println(status.retMessage);
                    System.out.println("Query is completed.");
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

    @Test
    public void format() {
        String hiveSparkConfKey = "asdf";
        String value = null;
        System.out.println(String.format("%s=%s", hiveSparkConfKey, value));
    }


}
