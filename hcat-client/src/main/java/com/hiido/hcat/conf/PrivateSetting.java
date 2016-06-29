/**
 * Apr 1, 2013
 */
package com.hiido.hcat.conf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.hiido.hcat.common.util.ConfigurationUtils;
import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.common.util.SystemUtils;
import com.hiido.suit.CipherUser;
import com.hiido.suit.SuitUser;
import com.hiido.suit.common.util.CipherUtils.Alg;

public final class PrivateSetting implements PublicConstant {
    private static final short PRIVAE_PERM = 0600;
    private static final short IGNORED_PERM = -1;
    public static final boolean NIX_SYSTEM;
    static {
        NIX_SYSTEM = !(SystemUtils.isMac() || SystemUtils.isWin());
    }
    private final String truststore;
    private final String truststorePasswd;

    private final CipherUser cuser;
    private final Configuration conf;

    private PrivateSetting(CipherUser cuser, String tstore, String tstorePasswd, Configuration conf) {
        this.cuser = cuser;
        this.truststore = genTruststore(tstore);
        this.truststorePasswd = tstorePasswd;
        this.conf = conf;
        conf.set(HIO_SUIT_TRUSTSTORE, truststore);

    }

    public static String genTruststore(String tstore) {
        String appHome = System.getProperty("app.home");
        String truststore = appHome == null ? tstore : appHome + '/' + tstore;
        return truststore;
    }

    public String getTruststore() {
        return truststore;
    }

    public String getTruststorePasswd() {
        return truststorePasswd;
    }

    public CipherUser getCipherUser() {
        return cuser;
    }

    public Configuration getPrivateConf() {
        return conf;
    }

    public static PrivateSetting instance(String file) throws IOException {
        File f = new File(file);
        Configuration conf = ConfigurationUtils.loadConfiguration(NIX_SYSTEM ? PRIVAE_PERM : IGNORED_PERM, f, false);
        return instance(conf);
    }

    public static PrivateSetting instance(Configuration conf) throws IOException {

        // read from "-token" option
        String tokenStr = System.getProperty(PublicConstant.HIO_KEYSTONE_TOKEN_PROPERTY);
        SuitUser suser = null;
        if (tokenStr == null) {
            String tokenFileName = conf.get(HIO_TOKEN_FILE);
            suser = TokenReader.readSuitUserInClasspath(tokenFileName);
        } else {
            suser = TokenReader.readSuitUser(new ByteArrayInputStream(tokenStr.getBytes()));
        }

        String truststore = conf.get(HIO_SUIT_TRUSTSTORE, DEF_SUIT_TRUSTSTORE);
        String truststorePasswd = conf.get(HIO_SUIT_TRUSTSTORE_PASSWD);

        if (StringUtils.isEmpty(truststore) || StringUtils.isEmpty(truststorePasswd)) {
            throw new IllegalArgumentException(String.format("%s or %s could not empty", HIO_SUIT_TRUSTSTORE,
                    HIO_SUIT_TRUSTSTORE_PASSWD));
        }

        int passwdMd5Count = conf.getInt(HIO_PASSWD_MD5_COUNT, 4);
        boolean cipherKey = conf.getBoolean(HIO_CIPHER_KEY, true);
        Alg alg = Alg.valueOf(conf.get(HIO_CIPHER_KEY_ALG, Alg.RC4.name()));
        CipherUser cuser = new CipherUser(suser, passwdMd5Count, alg, cipherKey);

        return new PrivateSetting(cuser, truststore, truststorePasswd, conf);
    }

}
