/**
 * Mar 7, 2013
 */
package com.hiido.hcat.common;

import java.nio.charset.Charset;
import java.util.EnumSet;

/**
 * @author lin
 * 
 */
public interface PublicConstant {
    String SITE_CORE = "private-core-site.xml";
    String SITE_MR = "private-mapred-site.xml";

    String METASTORE = "default";

    String PRIVATE_CONF = "private.xml";
    String HIO_HIVE_CLI = "hive-cli.xml";
    String HIO_HDFS_CLI = "hdfs-cli.xml";

    String HDFS_CACHE_DISABLE = "fs.hdfs.impl.disable.cache";
    String HTTPFS_DISABLE_CACHE_SSL = "fs.https.impl.disable.cache";
    String HTTPFS_DISABLE_CACHE = "fs.http.impl.disable.cache";

    Charset UTF8 = Charset.forName("UTF-8");

    String SYS_LOG_USER = "sys.log.user";
    String SYS_CUR_USER = "sys.cur.user";
    String SYS_UNKNOW_USER = "unknow";

    String HIO_SUIT_USER = "hio.suit.user";
    String HIO_SUIT_PASSWD = "hio.suit.passwd";
    String HIO_SUIT_TRUSTSTORE = "hio.suit.truststore";
    String DEF_SUIT_TRUSTSTORE = "ssl/hiohadoopcli.cer";
    String HIO_SUIT_TRUSTSTORE_PASSWD = "hio.suit.truststore.passwd";

    String HIO_FS_DEFAULT_NAME = "fs.default.name";
    String HIO_FS_EXT_SERVER = "httpfs.ext.server";

    String HIO_CIPHER_KEY = "hio.cipher.key";
    String HIO_CIPHER_KEY_ALG = "hio.cipher.key.alg";
    String HIO_PASSWD_MD5_COUNT = "hio.passwd.md5.count";

    String HIO_TOKEN_FILE = "hio.token.file.name";
    String HIO_CIPHER_ENABLE = "hio.cipher.enable";

    String HIO_DW_ID = "hio.dw.id";
    String HIO_TOKEN_EXPIRE_TIME = "hio.token.expire.time";
    String HIO_TOKEN_IP_LIST = "hio.token.ip.list";
    String HIO_TOKEN_UUID = "hio.token.uuid";

    String HIO_KEYSTONE_TOKEN_PROPERTY = "hio.keystone.token.in.property";

    String HIO_SERVICE_VER = "ver";

    String HTTP_CONN_TIMEOUT = "http.conn.timeout";
    String HTTP_READ_TIMEOUT = "http.read.timeout";

    String OCTET_CONTENT_TYPE = "application/octet-stream";
    String JSON_CONTENT_TYPE = "application/json";

    String ERROR_JSON = "err";
    String ERROR_RET_JSON = "ret";
    String ERROR_MSG_JSON = "msg";

    int HTTP_ACCEPT_ERR = 250;
    int HTTP_SOFT_REJECT = 251;

    String FS_CIPHER_ALG = "fs.cipher.alg";

    String FS_CIPHER_ALG_KEYLEN = "fs.cipher.alg.keylen";

    String FS_CHECKSUM_ALG = "fs.checksum.alg";

    String FS_SHELL_SILENT = "fs.shell.silent";

    String BEESWAX_SERVIVE = "/beeswax";

    String HCAT_GROUP = "hcat.group";
    String HCAT_USER = "hcat.user";
    String HCAT_QID = "hcat.qid";

    String HCAT_WARN_LEVEL = "hcat.warn.level";
    

    
    int HCAT_WARN_LEVEL_NON = 0;
    int HCAT_WARN_LEVEL_MAIL = 1;
    int HCAT_WARN_LEVEL_SNS = 2;

    int HCAT_WARN_LEVEL_DEF = HCAT_WARN_LEVEL_MAIL;
    
    String HCAT_ADD_SKIP = "hcat.add.skip";

    String HCAT_COMPRESS_INDEX_AUTO = "hcat.compress.index.auto";
    String HCAT_LOAD_COMPRESS = "hcat.load.compress";
    String HCAT_GET_UNCOMPRESS_AUTO = "hcat.get.uncompress.auto";
    String HCAT_INSERT_OVERWRITE_TABLE = "hcat.insert.overwrite.table";
    String HCAT_INSERT_INTO_TABLE = "hcat.insert.into.table";
    String HCAT_INDEX_TMP_DIR = "hcat.index.tmp.dir";
    
    String HCAT_COMPRESS_INDEX_MIN_SIZE = "hcat.compress.index.min.size";
    long DEF_HCAT_COMPRESS_INDEX_MIN_SIZE = 1024L*1024 *(128+64);

    String HCAT_GZ_NATIVE_ENABLE = "hcat.gz.native.enable";
    String MAPRED_OUTPUT_COMPRESS = "mapred.output.compress";
    
    String HCAT_LOCAL_MODE_STRICT_A3 = "hcat.local.mode.strict.a3";
    
    String HCAT_SKIP_SHOW_DDL_A3 = "hcat.skip.show.ddl.a3";

    public enum LBPolicy {
        Backup, RR
    }

    public enum FSAccess {
        None(0), MetaGet(1), MetaSet(2), Read(4), Write(8), Del(16);
        public final int bit;

        private FSAccess(int bit) {
            this.bit = bit;
        }

        public static EnumSet<FSAccess> enumSet(int bit) {
            if (bit < 0) {
                throw new IllegalArgumentException("native bit:" + bit);
            }
            if (bit == FSAccess.None.bit) {
                return EnumSet.of(FSAccess.None);
            }
            EnumSet<FSAccess> set = EnumSet.noneOf(FSAccess.class);
            for (FSAccess t : FSAccess.values()) {
                if ((t.bit & bit) != 0) {
                    set.add(t);
                }
            }
            return set;

        }
    }

}
