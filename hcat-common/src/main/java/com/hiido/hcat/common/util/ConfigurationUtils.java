/**
 * 2012-10-17
 */
package com.hiido.hcat.common.util;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

/**
 * @author lin
 * 
 */
public final class ConfigurationUtils {
    private static final Logger LOG = Logger.getLogger(ConfigurationUtils.class);

    private ConfigurationUtils() {
    }

    public static void putMap2Conf(Configuration conf, Map<String, String> map) {
        // just init the props
        conf.get("test");
        if (map == null) {
            return;
        }
        for (Entry<String, String> e : map.entrySet()) {
            conf.set(e.getKey(), e.getValue());
        }
    }

    public static void putProps2Conf(Configuration conf, Properties props) {
        // just init the props
        conf.get("test");
        Enumeration<String> en = (Enumeration<String>) props.propertyNames();
        while (en.hasMoreElements()) {
            String k = en.nextElement();
            conf.set(k, props.getProperty(k));
        }
    }

    public static void putConf2Conf(Configuration src, Configuration dst) {
        Iterator<Entry<String, String>> it = src.iterator();
        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            dst.set(e.getKey(), e.getValue());
        }
    }

    public static Map<String, String> conf2Map(Configuration conf) {
        Map<String, String> map = new HashMap<String, String>();
        Iterator<Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            String key = e.getKey();
            map.put(key, conf.get(key));
        }
        return map;
    }

    public static Map<String, String> getOverrideConf(Configuration o, Configuration n) {
        Map<String, String> override = new HashMap<String, String>();

        Iterator<Entry<String, String>> it = n.iterator();

        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            String k = e.getKey();
            String newVal = n.get(k);
            String oldVal = o.get(k);
            if (oldVal != null) {
                // old contains
                if (!oldVal.equals(newVal)) {
                    // override
                    override.put(k, newVal);
                }
            } else {
                // the new kv
                override.put(k, newVal);
            }
        }
        return override;
    }

    public static Map<String, String> getOverrideMap(Map<String, String> o, Map<String, String> n) {
        Map<String, String> override = new HashMap<String, String>();

        for (Entry<String, String> e : n.entrySet()) {
            String k = e.getKey();
            String newVal = e.getValue();
            String oldVal = o.get(k);
            if (oldVal != null) {
                // old contains
                if (!oldVal.equals(newVal)) {
                    // override
                    override.put(k, newVal);
                }
            } else {
                // the new kv
                override.put(k, newVal);
            }
        }
        return override;
    }

    public static String configurationPrintString(Configuration conf) {
        StringBuilder sb = new StringBuilder(512);
        Iterator<Entry<String, String>> it = conf.iterator();
        boolean first = true;
        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            if (first) {
                first = false;
            } else {
                sb.append(";");
            }
            String key = e.getKey();
            sb.append(key).append("=").append(conf.get(key));
        }
        return sb.toString();
    }

    public static Configuration loadConfiguration(short exceptPerm, File file, boolean loadDefault) throws IOException {
        if (!file.isFile()) {
            throw new IOException("not found the file:" + file.getAbsolutePath());
        }
        if (exceptPerm >= 0) {
            FsPermission perm = SystemUtils.getFsPermission(file);
            short fperm = perm.toShort();
            if (fperm != exceptPerm) {
                throw new IOException(String.format("except file[%s] perm[%o],but get[%o]", file.getName(), exceptPerm,
                        fperm));
            }
        }

        Configuration conf = new Configuration(loadDefault);
        conf.addResource(new Path(file.getAbsolutePath()));
        return conf;
    }

    public static void main(String[] args) throws Exception {
        String str = "rrwxrwxrwx";
        FsPermission p = FsPermission.valueOf(str);
        System.out.println(p);
    }

}
