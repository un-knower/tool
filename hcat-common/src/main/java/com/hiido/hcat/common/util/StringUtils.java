package com.hiido.hcat.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import com.hiido.hcat.common.err.ErrCodeException;

public final class StringUtils {
    private static final int DEF_STRING_SIZE = 1024;
    private static final ThreadLocal<Formatter> STRING_FORMATTER = new ThreadLocal<Formatter>() {
        @Override
        protected Formatter initialValue() {
            StringBuilder builder = new StringBuilder(DEF_STRING_SIZE);
            return new Formatter(builder);
        }
    };

    private static final ThreadLocal<char[]> CHAR_ARRAY = new ThreadLocal<char[]>() {
        @Override
        protected char[] initialValue() {
            char[] array = new char[4096];
            return array;
        }
    };

    private static final ThreadLocal<ObjectMapper> jsonObjectMapper = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper();
        }

    };

    private StringUtils() {
    }

    public static <T> T jsonFromInput(InputStream in, Class<T> clazz) throws Exception {
        ObjectMapper mapper = jsonObjectMapper.get();
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        return mapper.readValue(in, clazz);
    }

    public static <T> T jsonFromString(String str, Class<T> clazz) throws Exception {
        ObjectMapper mapper = jsonObjectMapper.get();
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        return mapper.readValue(str, clazz);

    }

    public static String obj2Json(Object obj) throws Exception {
        ObjectMapper mapper = jsonObjectMapper.get();
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        mapper.configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
        return mapper.writeValueAsString(obj);
    }

    public static byte[] obj2JsonAsBytes(Object obj) throws Exception {
        ObjectMapper mapper = jsonObjectMapper.get();
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        return mapper.writeValueAsBytes(obj);
    }

    public static char[] getCharArray() {
        return CHAR_ARRAY.get();
    }

    public static Formatter getStringFormatter() {
        Formatter f = STRING_FORMATTER.get();
        ((StringBuilder) f.out()).setLength(0);
        return f;
    }

    public static String format(String str, Object... objs) {
        Formatter f = getStringFormatter();
        return f.format(str, objs).toString();
    }

    public static String[] split(String str, String split) {
        List<String> strings = new ArrayList<String>(16);
        StringTokenizer token = new StringTokenizer(str, split);
        while (token.hasMoreElements()) {
            strings.add(token.nextToken());
        }
        String[] strArray = new String[strings.size()];
        return strings.toArray(strArray);
    }

    public static List<String> split2List(String str, String split) {
        List<String> strings = new ArrayList<String>(16);
        StringTokenizer token = new StringTokenizer(str, split);
        while (token.hasMoreElements()) {
            strings.add(token.nextToken());
        }
        return strings;
    }

    public static String array2String(Object[] objs, String split) {
        StringBuilder builder = new StringBuilder(256);
        for (int i = 0; i < objs.length; i++) {
            Object obj = objs[i];
            builder.append(obj.toString());
            if (i != objs.length) {
                builder.append(split);
            }
        }
        return builder.toString();
    }

    public static String list2String(List<String> objs, String split) {
        StringBuilder builder = new StringBuilder(256);
        for (int i = 0; i < objs.size(); i++) {
            String obj = objs.get(i);
            builder.append(obj);
            if (i != objs.size() - 1) {
                builder.append(split);
            }
        }
        return builder.toString();
    }

    public static Map<String, String> parseArgs(String argvStart, String[] args) {
        Map<String, String> rs = new HashMap<String, String>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            String v = ((i < args.length - 1) && !args[i + 1].startsWith(argvStart)) ? args[++i] : null;
            arg = arg.substring(argvStart.length());
            rs.put(arg, v);
        }
        return rs;
    }

    // public static void main(String[] args) {
    // Map<String, String> rs = parseArgs("-", new String[] { "-c", "1", "-a",
    // "1", "-b" });
    // System.out.println(rs);
    // }

    public static String trimHexString(String hex) {
        String s = hex.trim();
        if (s.startsWith("0x") || s.startsWith("0X"))
            s = s.substring(2);
        return s;
    }

    private static long parseLongBase(String s, int base) {
        int i = 0;
        int sign = 1;
        long r = 0;

        while (i < s.length() && Character.isWhitespace(s.charAt(i)))
            i++;
        if (i < s.length() && s.charAt(i) == '-') {
            sign = -1;
            i++;
        } else if (i < s.length() && s.charAt(i) == '+') {
            i++;
        }
        while (i < s.length()) {
            char ch = s.charAt(i);
            if ('0' <= ch && ch < '0' + base)
                r = r * base + ch - '0';
            else if ('A' <= ch && ch < 'A' + base - 10)
                r = r * base + ch - 'A' + 10;
            else if ('a' <= ch && ch < 'a' + base - 10)
                r = r * base + ch - 'a' + 10;
            else
                return r * sign;
            i++;
        }
        return r * sign;
    }

    public static byte[] parseHexString(String hex) {
        String s = trimHexString(hex);
        int length = s.length();
        if (length % 2 != 0) {
            s = "0" + s;
            length++;
        }

        byte[] arr = new byte[length / 2];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (byte) parseLongBase(s.substring(i * 2, 2 + i * 2), 16);
        return arr;
    }

    public static boolean isEmpty(String str) {
        if (str == null) {
            return true;
        }

        if ("".equals(str.trim())) {
            return true;
        }
        return false;
    }

    public static void checkEnpty(String str, String tag) {
        if (isEmpty(str)) {
            throw new IllegalArgumentException(tag + " could not empty");
        }
    }

    private static final char hexDigits[] = { // 用来将字节转换成 16 进制表示的字符
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    public static String md5(String str) {
        byte[] bytes = str.getBytes(UTF8_CHARSET);
        return md5(bytes);
    }

    public static String toHex(byte[] bytes16) {
        // 用字节表示就是 16 个字节
        char str[] = new char[16 * 2]; // 每个字节用 16 进制表示的话，使用两个字符，
        // 所以表示成 16 进制需要 32 个字符
        int k = 0; // 表示转换结果中对应的字符位置
        for (int i = 0; i < 16; i++) { // 从第一个字节开始，对 MD5 的每一个字节
            // 转换成 16 进制字符的转换
            byte byte0 = bytes16[i]; // 取第 i 个字节
            str[k++] = hexDigits[byte0 >>> 4 & 0xf]; // 取字节中高 4 位的数字转换,
            // >>>
            // 为逻辑右移，将符号位一起右移
            str[k++] = hexDigits[byte0 & 0xf]; // 取字节中低 4 位的数字转换
        }
        return new String(str); // 换后的结果转换为字符串
    }

    public static String md5(byte[] source) {

        java.security.MessageDigest md;
        try {
            md = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e1) {
            throw new RuntimeException(e1);
        }

        md.update(source);
        byte tmp[] = md.digest(); // MD5 的计算结果是一个 128 位的长整数，
                                  // 用字节表示就是 16 个字节
        return toHex(tmp);
    }

    public static String readAllString(InputStream in) throws IOException {
        StringBuilder buf = new StringBuilder(1024);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = reader.readLine()) != null) {
            buf.append(line).append('\n');
        }
        return buf.toString();
    }

    public static String timeString(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return sdf.format(time);
    }

    public static String nowString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return sdf.format(new Date(System.currentTimeMillis()));
    }

    public static String byteArrayString(byte[] array, String split) {
        StringBuilder sb = new StringBuilder(array.length * 2);
        for (byte b : array) {
            sb.append(b).append(split);
        }
        if (sb.length() != 0) {
            sb.setLength(sb.length() - split.length());
        }
        return sb.toString();
    }

    public static byte[] bytesOf(String s, String split) {
        String[] bsStr = s.split(split);
        byte[] bytes = new byte[bsStr.length];
        for (int i = 0; i < bsStr.length; i++) {
            bytes[i] = Byte.valueOf(bsStr[i].trim());
        }
        return bytes;
    }

    public static String asserNotEmpty(String str, String tag) {
        if (str == null || (str = str.trim()).length() == 0) {
            throw new IllegalArgumentException("string empty:" + tag);
        }
        return str;
    }

    public static List<URI> parseURI(String list) throws IOException {
        List<URI> uris = new ArrayList<URI>();
        if (list == null || (list = list.trim()).isEmpty()) {
            return uris;
        }

        String[] strs = list.split(";");

        for (String str : strs) {
            str = str.trim();
            try {
                URI u = new URI(str);
                int port = u.getPort();
                String addr = u.getHost() + (port > 0 ? (":" + port) : "");
                uris.add(new URI(u.getScheme() + "://" + addr));
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        }
        return uris;
    }

    public static String concat(List<String> list, String split) {
        StringBuilder sb = new StringBuilder(list.size() * 64);
        for (String str : list) {
            sb.append(str).append(split);
        }
        if (sb.length() != 0) {
            sb.setLength(sb.length() - split.length());
        }
        return sb.toString();
    }

    public static String addrPortIncrement(String target) {
        String[] strs = target.split(":");
        int port = Integer.valueOf(strs[strs.length - 1]);
        port++;
        String name = target.substring(0, target.length() - strs[strs.length - 1].length()) + String.valueOf(port);
        return name;
    }

    public static String readFileAsOneLine(InputStream in) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            StringBuilder builder = new StringBuilder();
            while ((line = br.readLine()) != null) {
                line = line.trim();
                builder.append(line);
            }
            return builder.toString();
        } catch (IOException e) {
            throw e;
        } finally {
            IOUtils.closeIO(br);
        }
    }

    public static String readFileAsOneLine(String filePath) throws IOException {
        BufferedReader br = null;
        try {
            File file = new File(filePath);
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line = null;
            StringBuilder builder = new StringBuilder();
            while ((line = br.readLine()) != null) {
                line = line.trim();
                builder.append(line);
            }
            return builder.toString();
        } catch (IOException e) {
            throw e;
        } finally {
            IOUtils.closeIO(br);
        }
    }

    public static String formatThrowable(Throwable e) {
        String msg = null;
        if (e instanceof ErrCodeException) {
            msg = String.format("code=%s:%s", ((ErrCodeException) e).errCode().name(), e.getMessage());
        } else {
            msg = String.format("%s:%s", e.getClass().getSimpleName(), e.getMessage());
        }
        return msg;
    }

    public static String stringifyException(Throwable e) {
        StringWriter stm = new StringWriter();
        PrintWriter wrt = new PrintWriter(stm);
        e.printStackTrace(wrt);
        wrt.close();
        return stm.toString();
    }

    public static String takeRight(String str,int len){
        if(str.length()<=len){
            return str;
        }
        //return str.substring(str.length()-len, len);
        return str.substring(str.length()-len);
    }
    public static void main(String[] args) throws Exception {
        // String h = "http://localhost:4888/a";
        // System.out.println(new URI(h).getAuthority());
        // System.out.println(new URI(h).getPath());
        // System.out.println(new URI(h).getHost());
        // String a = "032ce83b465499938dhg77d8bc9ef7fc";
        // for (int i = 0; i < 4; i++) {
        // a = md5(a);
        // }
        // System.out.println(a);
        //
        // List<String> ss = new ArrayList<String>();
        // ss.add("table1");
        // System.out.println(list2String(ss, ";"));
        // ss.add("table2");
        // System.out.println(list2String(ss, ";"));
        String a = "sdad:134";
        String b = "dasd:dad:22";
        System.out.println(addrPortIncrement(a));
        System.out.println(addrPortIncrement(b));

        String passwd = "933a240cb3fc0e485b85c2030ba82065";
        for (int i = 0; i < 4; i++) {
            passwd = StringUtils.md5(passwd);
        }
        System.out.println(passwd);
    }
}
