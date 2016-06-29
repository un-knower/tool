package com.hiido.hcat.common.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

public final class SystemUtils {
    private static final Executor DEF_EXEC = new DefaultExecutor();

    private static final String CMD_LOGNAME = "logname";
    private static final String CMD_WHOAMI = "whoami";

    private static final String CMD_LSL = "ls -l ";
    private static final String CMD_HLN = "ln '%s' '%s'";

    private static final Logger log = Logger.getLogger(SystemUtils.class);

    private static volatile Thread timer;
    private static volatile long time = System.currentTimeMillis();

    private static String logname;
    private static boolean haslogname;

    private static String whomi;
    private static boolean haswhomi;

    private static final String host;

    static {
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Throwable e) {
            hostname = "unknow";
        }
        host = hostname;
    }

    private SystemUtils() {
    }

    public static long vagueTime() {
        if (timer == null) {
            synchronized (SystemUtils.class) {
                if (timer == null) {
                    time = System.currentTimeMillis();
                    timer = new Thread(new Timer(), "sys_timer");
                    timer.setDaemon(true);
                    timer.start();
                }
            }
        }
        return time;
    }

    private static final class Timer implements Runnable {

        @Override
        public void run() {
            while (true) {
                time = System.currentTimeMillis();
                sleep(50);
            }

        }

    }

    public static boolean join(Thread t, long millis) {
        try {
            if (millis > 0) {
                t.join(millis);
            } else {
                t.join();
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    public static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    public static int exec(String cmd) throws Exception {
        return exec(cmd, System.out, System.err);
    }

    public static int exec(String cmd, OutputStream out, OutputStream err) throws Exception {
        return exec(cmd, out, err, false);
    }

    public static int exec(String cmd, OutputStream out, OutputStream err, boolean ignoreExitErr) throws Exception {
        Executor exec = new DefaultExecutor();
        if (ignoreExitErr) {
            exec.setExitValues(null);
        }
        CommandLine cl = CommandLine.parse(cmd);
        PumpStreamHandler streamHandler = new PumpStreamHandler(out, err);
        exec.setStreamHandler(streamHandler);
        try {
            streamHandler.start();
            int exitValue = exec.execute(cl);
            return exitValue;
        } finally {
            streamHandler.stop();
        }
    }

    public static int exec(String cmd, String outfile, OutputStream err) throws Exception {
        IOUtils.createFile(outfile, true);
        OutputStream out = new FileOutputStream(outfile);
        out = new BufferedOutputStream(out, 4096);

        try {
            return exec(cmd, out, err);
        } finally {
            IOUtils.closeIO(out);
        }
    }

    public static void execAndCheck(String cmd, OutputStream out, OutputStream err) throws Exception {
        int code = exec(cmd, out, err);
        if (DEF_EXEC.isFailure(code)) {
            throw new Exception(cmd + ":exec error,exitValue:" + code);
        }
    }

    public static void execAndCheck(String cmd) throws Exception {
        int code = exec(cmd);
        if (DEF_EXEC.isFailure(code)) {
            throw new Exception(cmd + ":exec error,exitValue:" + code);
        }
    }

    public static void execAndCheck(String cmd, String outfile, OutputStream err) throws Exception {
        int code = exec(cmd, outfile, err);
        if (DEF_EXEC.isFailure(code)) {
            throw new Exception(cmd + ":exec error,exitValue:" + code);
        }
    }

    public static void threadJoin(Thread... ts) {
        while (true) {
            int count = ts.length;
            for (Thread t : ts) {
                try {
                    t.join();
                    count--;
                } catch (InterruptedException e) {
                }
            }
            if (count == 0) {
                break;
            }
        }
    }

    public static String getLocalHostName() {
        return host;
    }

    public static String getLocalIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public static String getNetInterface(String name) {
        try {
            String ip = null;
            Enumeration<?> e1 = (Enumeration<?>) NetworkInterface.getNetworkInterfaces();
            while (e1.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) e1.nextElement();
                if (!ni.getName().equals(name)) {
                    continue;
                } else {
                    Enumeration<?> e2 = ni.getInetAddresses();
                    while (e2.hasMoreElements()) {
                        InetAddress ia = (InetAddress) e2.nextElement();
                        if (ia instanceof Inet6Address)
                            continue;
                        ip = ia.getHostAddress();
                    }
                    break;
                }
            }
            return ip;
        } catch (Exception e) {
            return null;
        }
    }

    public static List<String> getNetInterface(boolean acceptIP6, String... exclude) {
        Set<String> ips = new HashSet<String>();
        try {
            Enumeration<?> e1 = (Enumeration<?>) NetworkInterface.getNetworkInterfaces();
            while (e1.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) e1.nextElement();
                Enumeration<?> e2 = ni.getInetAddresses();
                while (e2.hasMoreElements()) {
                    InetAddress ia = (InetAddress) e2.nextElement();
                    boolean ip6 = ia instanceof Inet6Address;
                    if (!ip6 || acceptIP6) {
                        ips.add(ia.getHostAddress());
                    }
                }
            }
        } catch (Exception e) {
            log.error("err in get net interface:" + e.getMessage(), e);
        }
        for (String e : exclude) {
            ips.remove(e);
        }
        return new ArrayList<String>(ips);
    }
    
    public static String getNetInterfaceAsString(boolean acceptIP6, String... exclude) {
        List<String> ips = getNetInterface(acceptIP6,exclude);
        StringBuilder sb = new StringBuilder(ips.size() * 16);
        for (String ip : ips) {
            sb.append(ip).append(";");
        }
        if (sb.length() != 0) {
            sb.setLength(sb.length() - 1);
        }
        String localIP = sb.length() == 0 ? "unknow" : sb.toString();
        return localIP;
    }

    public static String getLogSysUser() {
        String user = System.getProperty("logname");
        if (user != null) {
            return user;
        }
        if (!haslogname) {
            LogOutputStreamCollector out = new LogOutputStreamCollector();
            LogOutputStreamCollector err = new LogOutputStreamCollector();
            try {
                exec(CMD_LOGNAME, out, err);
                logname = out.getString();
            } catch (Exception e) {
                log.debug(String.format("err in '%s':%s", CMD_LOGNAME, err.getString()));
            } finally {
                haslogname = true;
            }
        }
        return logname;
    }

    public static String getCurSysUser() {
        if (isWin()) {
            return "unknow";
        }
        if (!haswhomi) {
            LogOutputStreamCollector out = new LogOutputStreamCollector();
            LogOutputStreamCollector err = new LogOutputStreamCollector();
            try {
                exec(CMD_WHOAMI, out, err);
                whomi = out.getString();
            } catch (Exception e) {
                log.debug(String.format("err in '%s':%s", CMD_WHOAMI, err.getString()), e);
            } finally {
                haswhomi = true;
            }
        }
        return whomi;
    }

    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }

    public static FsPermission getFsPermission(File f) throws IOException {
        if (!f.exists()) {
            throw new IOException("not exists:" + f.getAbsolutePath());
        }
        String shell = CMD_LSL + String.format("'%s'", f.getAbsolutePath());
        LogOutputStreamCollector out = new LogOutputStreamCollector();
        LogOutputStreamCollector err = new LogOutputStreamCollector();
        try {
            exec(shell, out, err);
        } catch (Exception e) {
            throw new IOException("failed to exec:" + shell);
        }
        String lsl = out.getString();
        if (StringUtils.isEmpty(lsl)) {
            throw new IOException("empty result of:" + shell);
        }
        String[] strs = lsl.split(" ");

        String perm = strs[0].trim();
        // for the fucking CentOS
        if (perm.endsWith(".")) {
            perm = perm.substring(0, perm.length() - 1).trim();
        }
        return FsPermission.valueOf(perm);

    }

    public static void makeHLN(File src, File tar) throws Exception {
        if (!src.exists()) {
            throw new IOException("not exist:" + src.getAbsolutePath());
        }
        if (src.isDirectory() && tar.isFile()) {
            throw new IOException("could not ln dir to file");
        }
        if (src.isFile()) {
            shell(String.format(CMD_HLN, src.getAbsolutePath(), tar.getAbsolutePath()));
        } else {
            File[] list = src.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.isFile();
                }
            });
            for (File f : list) {
                makeHLN(f, tar);
            }
        }
    }

    private static void shell(String cmd) throws IOException {
        LogOutputStreamCollector out = new LogOutputStreamCollector();
        LogOutputStreamCollector err = new LogOutputStreamCollector();
        try {
            exec(cmd, out, err);
        } catch (Exception e) {
            throw new IOException(String.format("err in '%s':%s", cmd, err.getString()));
        }
    }

    public static boolean isWin() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("win") >= 0);
    }

    public static boolean isMac() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("mac") >= 0);
    }

    public static void printExitCallerStack() {
        Runtime.getRuntime().addShutdownHook(new TraceThread());
    }

    private static final class TraceThread extends Thread {

        public TraceThread() {
            super(new Runnable() {
                @Override
                public void run() {
                    // donothing
                }
            });
        }

        @Override
        public void start() {
            log.warn(new Exception("System.exit call stack"));
            super.start();
        }
    }

    public static void main(String[] args) throws Exception {
        // long id = getPID();
        // System.out.println(id);
        // System.out.println(getLocalIP());
        // System.out.println(getNetInterface("eth0"));
        //
        // System.out.println("log=" + getLogSysUser());
        // System.out.println("cur=" + getCurSysUser());
        //
        // String cmd = "logname";
        // StringBuffer cmdout = new StringBuffer();
        // log.info("执行命令：" + cmd);
        // Process process = Runtime.getRuntime().exec(cmd); // 执行一个系统命令
        // process.waitFor();
        // InputStream fis = process.getInputStream();
        // BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        // String line = null;
        // while ((line = br.readLine()) != null) {
        // cmdout.append(line).append(System.getProperty("line.separator"));
        // }
        // log.info("执行系统命令后的结果为：\n" + cmdout.toString());
        // Collection<String> ips = getNetInterface(false, "127.0.0.1");
        // System.out.println(ips);
        //
        // while (true) {
        // System.out.println(vagueTime());
        // Thread.sleep(10);
        // }
        // makeHLN(new File("/home/lin/temp/testa"), new
        // File("/home/lin/temp/a"));
//        System.out.println(getLogSysUser());
//        System.out.println(getCurSysUser());
//        System.out.println(getLogSysUser());
//        System.out.println(getCurSysUser());
//        System.out.println(getLocalHostName());
//        System.out.println(getNetInterface(false, ""));
        
        String cmd = "/home/lin/hadoopspace/hcat/binlocal/hive";
        exec(cmd);
    }
}
