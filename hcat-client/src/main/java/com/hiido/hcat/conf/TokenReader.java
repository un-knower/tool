package com.hiido.hcat.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.hiido.hcat.common.util.JsonUtils;
import com.hiido.suit.SuitUser;
import com.hiido.suit.common.util.CipherUtils;
import com.hiido.suit.common.util.StringUtils;
import com.hiido.suit.common.util.SystemUtils;

public class TokenReader implements PublicConstant {

    public static final boolean NIX_SYSTEM;
    public static final String T_BUSUSER = "busUser";
    public static final String T_DWUSER = "dwUser";
    public static final String T_UUID = "uuid";

    public static final String T_DELEGATE_MODE = "token.delegate.mode";
    public static final String T_DELEGATE_KEYSTONE = "token.delegate.keystone";
    static {
        NIX_SYSTEM = !(SystemUtils.isMac() || SystemUtils.isWin());
    }

    public static SuitUser readSuitUser(Configuration conf) throws IOException {
        if (conf.getBoolean(T_DELEGATE_MODE, false)) {
            String token = conf.get(T_DELEGATE_KEYSTONE);
            if (token == null) {
                throw new IOException("token.keystone could not be null in :" + T_DELEGATE_KEYSTONE);
            }
            return readSuitUser(token, null);
        } else {
            String hdfsTokenFileName = conf.get(HIO_TOKEN_FILE);
            return TokenReader.readSuitUserFromFile(hdfsTokenFileName);
        }
    }

    public static Map<String, String> getUserInfo(Configuration conf) {
        if (conf.getBoolean("token.super.mode", false)) {
            return null;
        }
        Map<String, String> map = new HashMap<String, String>();
        String bus = conf.get(T_BUSUSER);
        String dw = conf.get(T_DWUSER);
        if (bus == null) {
            throw new IllegalArgumentException("could not be null:" + T_BUSUSER);
        }
        if (dw == null) {
            throw new IllegalArgumentException("could not be null:" + T_DWUSER);
        }
        map.put(T_BUSUSER, bus);
        map.put(T_DWUSER, dw);
        return map;
    }

    public static Map<Object, Object> getTokenMap(String token) throws IOException {
        byte[] hd = CipherUtils.decodeBase64(token);
        String decode = new String(hd);
        Map<Object, Object> tokenInside = JsonUtils.jsonFrom(decode, Map.class);
        return tokenInside;
    }

    public static SuitUser readSuitUser(InputStream in) throws IOException {
        return readSuitUser(in, null);
    }

    public static SuitUser readSuitUserInClasspath(String filename) throws IOException {
        URL fileUrl = Thread.currentThread().getContextClassLoader().getResource(filename);
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(filename);
        return readSuitUser(in);
    }

    public static SuitUser readSuitUserFromFile(String filename) throws IOException {
        return readSuitUserFromFile(filename, null);
    }

    public static SuitUser readSuitUserFromFile(String filename, Map<String, String> map) throws IOException {
        return readSuitUser(new FileInputStream(new File(filename)), map);
    }

    public static SuitUser readSuitUser(String tokenStr, Map<String, String> map) throws IOException {
        Map<Object, Object> tokenMap = getTokenMap(tokenStr);
        if (tokenMap.get(T_DWUSER) == null || tokenMap.get(T_BUSUSER) == null || tokenMap.get(T_UUID) == null) {
            throw new IOException("invalid token.");
        }
        String dwId = (String) tokenMap.get(T_DWUSER);
        String busUser = (String) tokenMap.get(T_BUSUSER);
        String tokenUuid = (String) tokenMap.get(T_UUID);
        if (map != null) {
            if (map.containsKey(T_DWUSER)) {
                dwId = map.get(T_DWUSER);
            }
            if (map.containsKey(T_BUSUSER)) {
                busUser = map.get(T_BUSUSER);
            }
        }
        String hdfsToken = tokenStr;
        SuitUser suitUser = SuitUser.newInstance(busUser, dwId);
        suitUser.setToken(hdfsToken);
        suitUser.setTokenUuid(tokenUuid);
        return suitUser;
    }

    public static SuitUser readSuitUser(InputStream in, Map<String, String> map) throws IOException {
        try {
            String tokenStr = StringUtils.readFileAsOneLine(in);
            if (StringUtils.isEmpty(tokenStr)) {
                throw new IOException("token string is empty");
            }
            return readSuitUser(tokenStr, map);
        } catch (Exception e) {
            throw new IOException("err in read token", e);
        } finally {
            in.close();
        }
    }
}
