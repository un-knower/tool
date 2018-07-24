package com.hiido.hcat.service;

import com.hiido.hcat.common.util.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zrc on 17-8-13.
 */
public class HcatMultiNamenode {

    private final static String[] namenodes = {
            "hdfs://hcat3cluster",
            "hdfs://hcat4cluster",
            "hdfs://hcat7cluster"
    };

    private final static String[] config = {
            "hive.exec.scratchdir",
            "yarn.app.mapreduce.am.staging-dir",
            "hcat.exec.tmpdbdir"
    };
    private static AtomicInteger index = new AtomicInteger(0);

    static String getNamenode() {
        return namenodes[(index.getAndIncrement() & Integer.MAX_VALUE) % namenodes.length];
    }

    public static void configureHiveConf(HiveConf conf) {
        String namenode = conf.getBoolean("hcat.use.multiple.namenode", true) ? getNamenode() : conf.get("fs.defaultFS");
        for(String cc : config) {
            String defValue = conf.get(cc);
            if(defValue == null || defValue.startsWith("hdfs://"))
                continue;
            conf.set(cc, namenode + defValue);
        }

        String aux = conf.getVar(HiveConf.ConfVars.HIVEAUXJARS);
        if(StringUtils.isEmpty(aux) || aux.startsWith("hdfs://"))
            return;
        StringBuilder stringBuilder = new StringBuilder();
        for(String jar : aux.split(",")){
            if(stringBuilder.length()>0)
                stringBuilder.append(",");
            stringBuilder.append(namenode).append(jar);
        }
        conf.setVar(HiveConf.ConfVars.HIVEAUXJARS, stringBuilder.toString());
    }
}
