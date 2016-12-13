package com.hiido.hcat.databus.network;

import com.hiido.suit.net.http.protocol.HttpRawProtocol;
import com.hiido.suit.net.http.protocol.RawProtocolReply;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zrc on 16-12-13.
 */
public class HttpProtocol extends HttpRawProtocol {

    public static final String P_NAME = "service/write";

    private String v;// "0.0.1版本号",
    private String appId;// "应该申请id",
    private String appKey;// "应用申请key",
    private String serviceTypeKey;// "对应业务类型标识,如手yy首页推荐或者me月度活跃排行",

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppKey() {
        return appKey;
    }

    public void setAppKey(String appKey) {
        this.appKey = appKey;
    }

    public String getServiceTypeKey() {
        return serviceTypeKey;
    }

    public void setServiceTypeKey(String serviceTypeKey) {
        this.serviceTypeKey = serviceTypeKey;
    }

    private List<ValueMeta> valueMetas = new ArrayList<ValueMeta>();
    private List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();// 存放写入的值

    private MetaExt metaExt;

    public MetaExt getMetaExt() {
        return metaExt;
    }

    public void setMetaExt(MetaExt metaExt) {
        this.metaExt = metaExt;
    }

    public List<ValueMeta> getValueMetas() {
        return valueMetas;
    }

    public void setValueMetas(List<ValueMeta> valueMetas) {
        this.valueMetas = valueMetas;
    }

    public List<Map<String, Object>> getValues() {
        return values;
    }

    public void setValues(List<Map<String, Object>> values) {
        this.values = values;
    }

    public static class ValueMeta {

        public ValueMeta() {

        }

        private String name;
        private String dataType;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDataType() {
            return dataType;
        }

        public void setDataType(String dataType) {
            this.dataType = dataType;
        }

    }

    public static class MetaExt {

        public static final String REWRITE_META_COLUMNS_Y = "Y";
        public static final String REWRITE_META_COLUMNS_N = "N";

        public static final String DELETE_RECORD_Y = "Y";
        public static final String DELETE_RECORD_N = "N";

        private String store_timeout;// 数据存入存储中，保留时间
        private String rewrite_meta_columns = REWRITE_META_COLUMNS_N;// true 重置字段元数据
        private String delete_record = DELETE_RECORD_N; // 删除数据

        public MetaExt() {

        }

        public MetaExt(String store_timeout, String rewrite_meta_columns, String delete_record) {
            this.store_timeout = store_timeout;
            this.rewrite_meta_columns = rewrite_meta_columns;
            this.delete_record = delete_record;
        }



        public String getStore_timeout() {
            return store_timeout;
        }

        public void setStore_timeout(String store_timeout) {
            this.store_timeout = store_timeout;
        }

        public void setRewrite_meta_columns(String rewrite_meta_columns) {
            this.rewrite_meta_columns = rewrite_meta_columns;
        }

        public boolean checkIssRewrite_meta_columns() {
            return REWRITE_META_COLUMNS_Y.equalsIgnoreCase(this.rewrite_meta_columns);
        }

        public String getRewrite_meta_columns() {
            return rewrite_meta_columns;
        }

        public String getDelete_record() {
            return delete_record;
        }

        public void setDelete_record(String delete_record) {
            this.delete_record = delete_record;
        }

        @Override
        public String toString() {
            return "MetaExt [store_timeout=" + store_timeout + ", rewrite_meta_columns=" + rewrite_meta_columns + "]";
        }

    }

    @Override
    public String name() {
        return P_NAME;
    }

    public static final class Reply extends RawProtocolReply {

        public static final int SUCCESS = 0;

        private int errcode;// int 错误编码",
        private String msg; // 错误信息",
        private int ret = 0;// "返回值，0-成功，非0-失败",

        public int getErrcode() {
            return errcode;
        }

        public void setErrcode(int errcode) {
            this.errcode = errcode;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public int getRet() {
            return ret;
        }

        public void setRet(int ret) {
            this.ret = ret;
        }

        public boolean isSuccess() {
            return ret == SUCCESS;
        }

        @Override
        public String toString() {
            return "Reply [errcode=" + errcode + ", msg=" + msg + ", ret=" + ret + "]";
        }

    }
}
