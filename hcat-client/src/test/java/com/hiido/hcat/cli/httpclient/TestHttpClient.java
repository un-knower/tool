package com.hiido.hcat.cli.httpclient;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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

    static String serveltUrl = "http://14.17.109.45:26025/query";

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
        String qid = "0_0_0_0_26025_20170208164722306_0";
        //serveltUrl = "http://14.17.109.45:26023/query";
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
        while (status.retCode == 0 && status.getQueryProgress().state < 1) {
            for (String job : status.getQueryProgress().jobId) {
                System.out.println("job id:" + job);
            }
            System.out.println("progress: " + status.getQueryProgress().progress);
            System.out.println("res:" + status.getQueryProgress().res == null ? "null" : status.getQueryProgress().res);
            System.out.println("fetch :" + status.getQueryProgress().getFetchDirs() == null ? "null" : status.getQueryProgress().getFetchDirs().get(0));
            Thread.sleep(5000);
            status = client.queryJobStatus(new QueryStatus(cipher, qid));
        }

        System.out.println("status:" + status.getQueryProgress().getState());
        System.out.println("progress: " + status.getQueryProgress().progress);
        System.out.println("res:" + status.getQueryProgress().res == null ? "null" : status.getQueryProgress().res);

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
        for (int i = 0; i < 1; i++) {
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

                sql = "set hive.execution.engine=spark;\n" +
                        //"set hcat.databus.ervice.type.key=abc;\n" +
                        "select\n" +
                        "    uid\n" +
                        "    ,count(distinct dt) as login_days\n" +
                        "from \n" +
                        "    bproduct.bproduct_me_mbsdk_do\n" +
                        "where\n" +
                        "    dt >= '20161130'\n" +
                        "and dt <= '20161214'\n" +
                        "group by uid";


                sql = "USE hiidosdk;\n" +
                        //"set hive.optimize.correlation=true;\n" +
                        //"set hive.auto.convert.join=false;\n" +
                        "set hive.execution.engine=spark;\n" +
                        //"set hive.optimize.sampling.orderby=true;\n" +
                        "create temporary function ipmon as 'com.hiido.hive.udf.IPMON';\n" +
                        "WITH sub_table AS (\n" +
                        "\tSELECT sys,IF((hdid!='' AND hdid IS NOT NULL AND algo=1) OR algo=2,hdid,CONCAT(imei,\"|\",mac) ) AS tid,country,province FROM (\n" +
                        "\t\tSELECT appkey,sys,hdid,imei,mac,net,ipmon(ip)[0] AS country,ipmon(ip)[1] AS province FROM default.yy_mbsdkdo_original WHERE dt >= '20161001' AND dt <= '20161002'\n" +
                        "\t)a LEFT JOIN (\n" +
                        "\t\tSELECT product_key,algo FROM hiidosdk.m_product\n" +
                        "\t)b ON a.appkey=b.product_key\n" +
                        "  \tWHERE b.product_key IS NOT NULL\n" +
                        ")\n" +
                        "\n" +
                        "select\n" +
                        "  '20161001' AS dt,sys,country,province,COUNT(DISTINCT tid) AS  client_count\n" +
                        "from\n" +
                        "  sub_table\n" +
                        "group by\n" +
                        "  sys,country,province;";


                sql =   "set hive.execution.engine=spark;\n" +
                        "select livetype,biz,anchorid,a.sid,\n" +
                        "from_unixtime(start_time,'yyyy-MM-dd HH:mm:ss') as start_time,\n" +
                        "from_unixtime(end_time,'yyyy-MM-dd HH:mm:ss') as end_time,\n" +
                        "live_duration,uid,suid,logtype,type,\n" +
                        "from_unixtime(st,'yyyy-MM-dd HH:mm:ss') as user_in,\n" +
                        "from_unixtime(ed,'yyyy-MM-dd HH:mm:ss') as user_out,\n" +
                        "case when st<start_time and ed>end_time then end_time-start_time\n" +
                        "when st<start_time and ed<=end_time then ed-start_time\n" +
                        "when st>=start_time and ed>end_time then end_time-st\n" +
                        "when st>=start_time and ed<=end_time then ed-st\n" +
                        "else 0 end as view_duration,source,ver,btype\n" +
                        "from\n" +
                        "(select type as livetype,if(biz='未知','other',biz) as biz,uid as anchorid,channel as sid,cast(unix_timestamp(start_time) as INT) as start_time,cast(unix_timestamp(end_time) as INT) as end_time,duration as live_duration from yule.yule_lkq_live_all_detail_day where dt='2017-01-16') a\n" +
                        "left outer join\n" +
                        "(select suid,uid,logtype,type,sid,b_time as st,e_time as ed,source,ver,btype from yule.yule_zyz_dr_original_day where dt='2017-01-16') b\n" +
                        "on a.sid=b.sid\n" +
                        "where ((st>=start_time and st<=end_time) or (ed>=start_time and ed<=end_time) or (st<start_time and ed>end_time));";

                sql =   //"set hive.execution.engine=spark;\n" +
                        //"set hive.merge.mapredfiles=true;\n" +
                        //"insert overwrite table freshman.freshman_mbyy_dm_hot_click_sum_hour partition(dt = '20170208',hour = '23')\n" +
                        "select\n" +
                        "     op_type\n" +
                        "    ,ver\n" +
                        "    ,count(distinct case when act_type = 1 then imac end) as show_uid_num\n" +
                        "    ,sum(case when act_type = 1 then click_cnt else 0 end) as show_cnt\n" +
                        "    ,count(distinct case when act_type = 2 then imac end) as click_uid_num\n" +
                        "    ,sum(case when act_type = 2 then click_cnt else 0 end) as click_cnt\n" +
                        "from\n" +
                        "(\n" +
                        "    select\n" +
                        "         split(concat('all_type#',op_type),'#') as op_types \n" +
                        "        ,split(concat('all_type#',ver),'#') as vers\n" +
                        "        ,act_type\n" +
                        "        ,imac\n" +
                        "        ,click_cnt\n" +
                        "    from\n" +
                        "        yule.yule_mbyy_ods_user_hot_act_hour_1h\n" +
                        "    where\n" +
                        "        dt = '20170207'\n" +
                        "    and hour <= '23'\n" +
                        ") a\n" +
                        "lateral view explode(op_types) mtb1 as op_type\n" +
                        "lateral view explode(vers) mtb2 as ver\n" +
                        "group by op_type,ver\n" +
                        ";";

                Map<String, String> cipher = cipher();
                Map<String, String> conf =  new HashMap();
                //conf.put("hcat.query.return.fetch", "true");

                CommitQuery cq = new CommitQuery().setQuery(sql).setCipher(cipher).setConf(conf);
                CommitQueryReply reply = client.commit(cq);
                if (reply.getHandle().isQuick())
                    //assertEquals(reply.getStatus(), JobStatus.COMPLETE);
                    System.out.println(reply.getHandle().running);
                else {
                    String qid = reply.getHandle().getQueryId();
                    System.out.println(qid + " started");
                    QueryStatusReply status = client.queryJobStatus(new QueryStatus(cipher, qid));

                    while (status.retCode == 0 && status.getQueryProgress().state <= 1) {
                        for (String job : status.getQueryProgress().jobId) {
                            System.out.println("job id:" + job);
                        }
                        System.out.println("progress: " + status.getQueryProgress().progress);
                        System.out.println("res:" + status.getQueryProgress().res == null ? "null" : status.getQueryProgress().res);
                        Thread.sleep(5000);
                        status = client.queryJobStatus(new QueryStatus(cipher, qid));
                    }
                    //System.out.println("fetch :" + status.getQueryProgress().getFetchDirs() == null ? "null" : status.getQueryProgress().getFetchDirs().get(0));

                    if (status.retMessage != null)
                        System.out.println(status.retMessage);
                    System.out.println("Query is completed.");
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
    public void cancel() {
        try {
            String jobId = "0_0_0_0_26025_20170222173510728_1";
            THttpClient thc = new THttpClient(serveltUrl);
            TProtocol lopFactory = new TBinaryProtocol(thc);
            CliService.Client client = new CliService.Client(lopFactory);
            CancelQuery request = new CancelQuery(cipher(), jobId);
            CancelQueryReply reply = client.cancelJob(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void format() {
        String hiveSparkConfKey = "asdf";
        String value = null;
        System.out.println(String.format("%s=%s", hiveSparkConfKey, value));
    }

    private Map<String, String> cipher() {
        HashMap<String, String> cipher = new HashMap<String, String>();
        cipher.put("loguser", "dw_zouruochen");
        cipher.put("curuser", "dw_zouruochen");
        cipher.put("bususer", "yule");
        cipher.put("skey", "6VdbVqlSi2uZwPXW+TIc8MI=");
        cipher.put("username", "zouruochen");
        cipher.put("user_id", "471");
        cipher.put("company_id", "189");
        return cipher;
    }


}
