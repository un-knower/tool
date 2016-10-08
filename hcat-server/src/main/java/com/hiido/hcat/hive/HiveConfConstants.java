/**
 * 2012-10-13
 */
package com.hiido.hcat.hive;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.hiido.hcat.common.PublicConstant;


/**
 * @author lin
 * 
 */
public final class HiveConfConstants implements PublicConstant {

    private HiveConfConstants() {
    }

    public static final String ZK_ZNODE_PARENT = "zookeeper.znode.parent";

    public static final String HCAT_CONTEXT_ISHDFSClEANUP = "hcat.context.ishdfscleanup";
    
    public static final String HIO_HIVE_METASTORE = "hio.hive.metastore";
    public static final String HIO_HIVE_USING_DB = "hio.hive.using.db";
    public static final String DEF_HIO_HIVE_USING_DB = "default";
    public static final String DEF_HIO_HIVE_METASTORE = "default";

    public static final String HIO_HIVE_SCHEDULE_PRIORITY = "hio.hive.schedule.priority";
    public static final int DEF_HIO_HIVE_SCHEDULE_PRIORITY = 4;

    public static final String HIO_HIVE_SCHEDULE_SLOT = "hio.hive.schedule.slot";
    public static final int DEF_HIO_HIVE_SCHEDULE_SLOT = 1;

    public static final String HIO_HIVE_SCHEDULE_NAMESPACE = "hio.hive.schedule.namespace";

    public static final String HIO_HIVE_JSON_FETCH = "hio.hive.json.fetch";
    public static final String HIVE_JOBNAME_PREFIX = "hive.jobname.prefix";

    public static final String HIVE_HISTORY_LOG_UNABLE = "hive.history.log.unable";
    public static final boolean DEF_HIVE_HISTORY_LOG_UNABLE = true;

    public static final String HIVE_CLI_PRINT_VERTICAL = "hive.cli.print.vertical";
    public static final boolean DEF_HIVE_CLI_PRINT_VERTICAL = false;

    public static final String HIVE_CLI_PRINT_VERTICAL_NUM = "hive.cli.print.vertical.num";
    public static final int DEF_HIVE_CLI_PRINT_VERTICAL_NUM = 1;

    public static final String HIVE_CLI_PRINT_VERTICAL_SPLITLINE = "hive.cli.print.vertical.splitline";
    public static final boolean DEF_HIVE_CLI_PRINT_VERTICAL_SPLITLINE = true;

    public static final String HIVE_CARTESIAN_PRODUCT_STRICT = "hive.cartesian.product.strict";
    public static final boolean DEF_HIVE_CARTESIAN_PRODUCT_STRICT = true;

    public static final String HIVE_ORDERBY_STRICT = "hive.orderby.strict";
    public static final boolean DEF_HIVE_ORDERBY_STRICT = true;

    public static final String HIVE_PARTITION_QUERY_STRICT = "hive.partition.query.strict";
    public static final boolean DEF_HIVE_PARTITION_QUERY_STRICT = true;

    public static final String HIVE_PERFLOG_UNABLE = "hive.perflog.unable";

    public static final String HIVE_TRANSLATE_QUERY = "hive.translate.query";
    
    public static final String HIVE_VERIFY_VIEW_WITHOUT_TABLE = "hive.verify.view.without.table";
    public static final boolean DEF_HIVE_VERIFY_VIEW_WITHOUT_TABLE = true;
    
    public static final String HCAT_VERIFY_UDFS = "hcat.verify.udfs";
    public static final String DEF_HCAT_VERIFY_UDFS = "";
    
    public static final String HCAT_SKEY_AUTH_UDFS = "hcat.skey.auth.udfs";
    public static final String DEF_HCAT_SKEY_AUTH_UDFS = "";

    public static final String HCAT_kEYSTORE_PATH = "hcat.keystore.path";
    public static final String HCAT_TRUSTSTORE_PATH = "hcat.truststore.path";
    public static final String HCAT_KEYSTORE_PASS = "hcat.keystore.pass";
    public static final String HCAT_TRUSTSTORE_PASS = "hcat.truststore.pass";
    public static final String HCAT_HVASERVER = "hcat.hvaserver";

    private static final Set<String> localIgnoreConf = new HashSet<String>();
    static {
        localIgnoreConf.add(HIO_SUIT_USER);
        localIgnoreConf.add(HIO_SUIT_PASSWD);
        localIgnoreConf.add(HIO_HIVE_METASTORE);
        localIgnoreConf.add(HIO_HIVE_USING_DB);
        localIgnoreConf.add(HIO_SUIT_TRUSTSTORE);
        localIgnoreConf.add(HIO_SUIT_TRUSTSTORE_PASSWD);

        // localIgnoreConf.add(HIO_HIVE_JSON_FETCH);
        // localIgnoreConf.add(HIO_HIVE_SCHEDULE_PRIORITY);
        // localIgnoreConf.add(HIO_HIVE_SCHEDULE_SLOT);
        // localIgnoreConf.add(HIO_HIVE_SCHEDULE_NAMESPACE);
    }

    public static boolean isLocalIgnoreConf(String name) {
        return localIgnoreConf.contains(name);
    }

    public static void checkSchedulePriority(String tag, int priority) {
        if (priority < 0) {
            throw new IllegalArgumentException(String.format("%s could not less than 0:%d", tag, priority));
        }
    }

    public static void checkScheduleSlot(String tag, int slot) {
        if (slot <= 0) {
            throw new IllegalArgumentException(String.format("%s must more than 0:%d", tag, slot));
        }
    }

    public static int getHivePriority(Configuration conf) {
        int priority = conf.getInt(HiveConfConstants.HIO_HIVE_SCHEDULE_PRIORITY,
                HiveConfConstants.DEF_HIO_HIVE_SCHEDULE_PRIORITY);
        HiveConfConstants.checkSchedulePriority(HiveConfConstants.HIO_HIVE_SCHEDULE_PRIORITY, priority);
        return priority;
    }

    public static int getHiveSlot(Configuration conf) {
        int slot = conf.getInt(HiveConfConstants.HIO_HIVE_SCHEDULE_SLOT, HiveConfConstants.DEF_HIO_HIVE_SCHEDULE_SLOT);
        HiveConfConstants.checkScheduleSlot(HiveConfConstants.HIO_HIVE_SCHEDULE_SLOT, slot);
        return slot;
    }

    public static String getHiveNamespace(Configuration conf) {
        return conf.get(HiveConfConstants.HIO_HIVE_SCHEDULE_NAMESPACE);
    }

    public static boolean getHiveJsonFetch(Configuration conf) {
        return Boolean.valueOf(conf.get(HiveConfConstants.HIO_HIVE_JSON_FETCH, "true"));
    }

    public static boolean isHistoryLogUnable(Configuration conf) {
        return conf.getBoolean(HIVE_HISTORY_LOG_UNABLE, DEF_HIVE_HISTORY_LOG_UNABLE);
    }

    public static boolean isCliPrintVertical(Configuration conf) {
        return conf.getBoolean(HIVE_CLI_PRINT_VERTICAL, DEF_HIVE_CLI_PRINT_VERTICAL);
    }

    public static int getCliPrintVerticalNum(Configuration conf) {
        return conf.getInt(HIVE_CLI_PRINT_VERTICAL_NUM, DEF_HIVE_CLI_PRINT_VERTICAL_NUM);
    }

    public static boolean isCliPrintVerticalSplitline(Configuration conf) {
        return conf.getBoolean(HIVE_CLI_PRINT_VERTICAL_SPLITLINE, DEF_HIVE_CLI_PRINT_VERTICAL_SPLITLINE);
    }

    public static boolean isOrderbyStrict(Configuration conf) {
        return conf.getBoolean(HIVE_ORDERBY_STRICT, DEF_HIVE_ORDERBY_STRICT);
    }

    public static boolean isCartesianProductStrict(Configuration conf) {
        return conf.getBoolean(HIVE_CARTESIAN_PRODUCT_STRICT, DEF_HIVE_CARTESIAN_PRODUCT_STRICT);
    }

    public static boolean isPartitionQueryStrict(Configuration conf) {
        return conf.getBoolean(HIVE_PARTITION_QUERY_STRICT, DEF_HIVE_PARTITION_QUERY_STRICT);
    }

	public static boolean getHiveVerifyViewWithoutTable(Configuration conf) {
		return conf.getBoolean(HIVE_VERIFY_VIEW_WITHOUT_TABLE, DEF_HIVE_VERIFY_VIEW_WITHOUT_TABLE);
	}
	
	public static String getHcatVerifyUDFs(Configuration conf) {
	    return conf.getTrimmed(HCAT_VERIFY_UDFS, DEF_HCAT_VERIFY_UDFS);
	}
	
	public static String getHcatSkeyAuthUDFs(Configuration conf){
	    return conf.getTrimmed(HCAT_SKEY_AUTH_UDFS, DEF_HCAT_SKEY_AUTH_UDFS);
	}

    public static String getHcatkeystorePath(Configuration conf) {
        return conf.get(HCAT_kEYSTORE_PATH, null);
    }

    public static String getHcatTruststorePath(Configuration conf) {
        return conf.get(HCAT_TRUSTSTORE_PATH, null);
    }

    public static String getHcatKeystorePass(Configuration conf) {
        return conf.get(HCAT_KEYSTORE_PASS, "");
    }

    public static String getHcatTruststorePass(Configuration conf) {
        return conf.get(HCAT_TRUSTSTORE_PASS, "");
    }

    public static String getHcatHvaserver(Configuration conf) {
        return conf.get(HCAT_HVASERVER);
    }

}
