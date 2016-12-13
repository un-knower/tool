package com.hiido.hcat.databus.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.FetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zrc on 16-12-12.
 * see org.apache.hadoop.hive.serde2.thrift.ThriftFormatter
 */
public class DatabusFormatter implements FetchFormatter<Object> {

    @Override
    public void initialize(Configuration configuration, Properties properties) throws Exception {
    }

    @Override
    public Object convert(Object row, ObjectInspector rowOI) throws Exception {
        StructObjectInspector structOI = (StructObjectInspector) rowOI;
        List<? extends StructField> fields = structOI.getAllStructFieldRefs();
        Map<String, Object> data = new HashMap<String, Object>();
        for (int i = 0 ; i < fields.size(); i++) {
            StructField fieldRef = fields.get(i);
            Object field = structOI.getStructFieldData(row, fieldRef);
            //null 值不再按照hive 默认的处理方式
            if(field == null)
                data.put(fieldRef.getFieldName(), null);
            else {
                ObjectInspector fieldInspector = fieldRef.getFieldObjectInspector();
                String value = SerDeUtils.getJSONString(field, fieldInspector);
                data.put(fieldRef.getFieldName(), value);
            }
        }
        return data;
    }

    @Override
    public void close() throws IOException {
    }
}
