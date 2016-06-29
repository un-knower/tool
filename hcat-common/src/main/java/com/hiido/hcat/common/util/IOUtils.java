package com.hiido.hcat.common.util;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;

import org.apache.hadoop.io.Text;

import com.hiido.hcat.common.io.HDFSLineReader;
import com.hiido.hcat.common.io.LineReader;

public final class IOUtils {

    private IOUtils() {
    }

    public static boolean isDirEmpty(String dir) {
        return isDirEmpty(new File(dir));
    }

    public static boolean isDirEmpty(File dir) {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("no dir:" + dir.getAbsolutePath());
        }
        return dir.list().length == 0;
    }

    public static List<String> pathList(String path) {
        File f = new File(path);
        List<String> list = new ArrayList<String>();
        while (f.getParentFile() != null) {
            list.add(f.getParent());
            f = f.getParentFile();
        }
        Collections.reverse(list);
        return list;
    }

    public static String path(String parent, String child) {
        File f = new File(parent, child);
        return f.getAbsolutePath();
    }

    public static void closeIO(Object obj) {
        if (obj == null) {
            return;
        }
        if (obj instanceof Closeable) {
            try {
                ((Closeable) obj).close();
            } catch (Exception e) {
            }
            return;
        }
        if (obj instanceof ResultSet) {
            try {
                ((ResultSet) obj).close();
            } catch (Exception e) {
            }
            return;
        }

        if (obj instanceof Statement) {
            try {
                ((Statement) obj).close();
            } catch (Exception e) {
            }
            return;
        }

        if (obj instanceof Connection) {
            try {
                ((Connection) obj).close();
            } catch (Exception e) {
            }
            return;
        }

        if (obj instanceof PreparedStatement) {
            try {
                ((PreparedStatement) obj).close();
            } catch (Exception e) {
            }
            return;
        }

        throw new IllegalArgumentException("unsupported close obj:" + obj.getClass());
    }

    public static boolean existsFile(String f) {
        return new File(f).exists();
    }

    public static void delete(String f) throws IOException {
        delete(new File(f));
    }

    public static void deleteNoErr(File f) {
        try {
            delete(f);
        } catch (IOException e) {
        }
    }

    public static void delete(File f) throws IOException {
        if (!f.exists()) {
            return;
        }
        if (f.isFile()) {
            if (!f.delete()) {
                throw new IOException("failed to delete the file:" + f.getAbsolutePath());
            }
            return;
        }
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (File child : files) {
                delete(child);
            }
            if (!f.delete()) {
                throw new IOException("failed to delete the dir:" + f.getAbsolutePath());
            }
        }
    }

    public static void echo(File f, String str, boolean sync) throws IOException {
        echo(f, str, "utf-8", sync);
    }

    public static void echo(File f, String str, String charset, boolean sync) throws IOException {
        createFile(f, false);

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(f);
            OutputStreamWriter outW = (charset != null ? new OutputStreamWriter(out, charset) : new OutputStreamWriter(
                    out));
            PrintWriter w = new PrintWriter(outW);
            w.write(str);
            w.flush();
            if (sync) {
                out.getFD().sync();
            }
        } finally {
            IOUtils.closeIO(out);
        }
    }

    public static FileChannel getFileChannel(File f) throws IOException {
        FileOutputStream fout = new FileOutputStream(f);
        return fout.getChannel();
    }

    public static void truncateFile(File f, long size) throws IOException {
        RandomAccessFile rand = new RandomAccessFile(f, "rw");
        try {
            rand.getChannel().truncate(size);
        } finally {
            rand.close();
        }
    }

    public static void createFile(File f, boolean overwrite) throws IOException {
        String path = f.getAbsolutePath();
        if (f.exists()) {
            if (overwrite) {
                delete(f);
            } else if (f.isDirectory()) {
                throw new IOException("the named file exists:" + path);
            } else {
                return;
            }
        }
        File parent = f.getParentFile();
        parent.mkdirs();
        if (!f.createNewFile()) {
            throw new IOException("failed to create file:" + path);
        }
    }

    public static File createFile(String f, boolean overwrite) throws IOException {
        File file = new File(f);
        IOUtils.createFile(file, true);
        return file;
    }

    public static File createDir(String dir, boolean overwrite) throws IOException {
        File fdir = new File(dir);
        createDir(fdir, overwrite);
        return fdir;
    }

    public static void createDir(File dir, boolean overwrite) throws IOException {
        String path = dir.getAbsolutePath();
        if (dir.exists()) {
            if (overwrite) {
                delete(dir);
            } else if (dir.isFile()) {
                throw new IOException("the named dir exists:" + path);
            } else {
                return;
            }
        }
        if (!dir.mkdirs()) {
            throw new IOException("failed to create dir:" + path);
        }
    }

    public static Properties createProps(String path) throws IOException {
        File f = new File(path);
        if (!f.exists() || f.isDirectory()) {
            throw new IOException("failed to find the file:" + f.getAbsolutePath());
        }
        OrderProperties props = new OrderProperties();
        InputStream in = new BufferedInputStream(new FileInputStream(f));
        try {
            if (path.endsWith(".xml")) {
                props.loadFromXML(in);
            } else {
                props.load(in);
            }
        } finally {
            IOUtils.closeIO(in);
        }
        return props;
    }

    public static void catString4File(File file, PrintStream printStream) throws IOException {
        if (!file.exists() || !file.isFile()) {
            throw new IOException("not find the file:" + file.getAbsolutePath());
        }
        if (file.length() == 0) {
            return;
        }
        LineReader reader = new HDFSLineReader(file.toURI().toURL().toString(), "utf-8");
        reader.open();
        try {
            String line = null;
            while ((line = reader.readLine()) != null) {
                printStream.println(line);
            }
        } finally {
            IOUtils.closeIO(reader);
        }
    }

    public static void catString(InputStream in, PrintStream printStream, int bufSize) throws IOException {
        org.apache.hadoop.util.LineReader reader = new org.apache.hadoop.util.LineReader(in, bufSize);
        try {
            Text text = new Text();
            while (reader.readLine(text) != 0) {
                printStream.println(text.toString());
                text.clear();
            }
        } finally {
            IOUtils.closeIO(reader);
        }
    }

    public static List<File> listFile(File f, boolean recursion) throws IOException {
        List<File> flist = new ArrayList<File>();
        if (!f.isDirectory()) {
            return flist;
        }

        File[] files = f.listFiles();
        for (File file : files) {
            if (file.isFile()) {
                flist.add(file);
            } else if (file.isDirectory() && recursion) {
                flist.addAll(listFile(file, true));
            }
        }
        return flist;
    }

    public static long copyBytes(InputStream in, OutputStream out, int buffSize, Progress p) throws IOException {
        byte buf[] = new byte[buffSize];
        return copyBytes(in, out, buf, p);
    }

    public static long copyBytes(InputStream in, OutputStream out, byte[] buf, Progress p) throws IOException {
        int bytesRead = -1;
        long count = 0;
        while ((bytesRead = in.read(buf)) > 0) {
            out.write(buf, 0, bytesRead);
            count += bytesRead;
            if (p != null) {
                p.report(count);
            }
        }
        return count;
    }

    public static long copyBytes(InputStream in, OutputStream out, int buffSize) throws IOException {
        return copyBytes(in, out, buffSize, null);
    }

    public static long copyBytes(InputStream in, OutputStream out, byte[] buf) throws IOException {
        return copyBytes(in, out, buf, null);
    }

    public static int readFully(InputStream in, byte[] buf, int len) throws IOException {
        int pos = 0;
        while (pos < len) {
            int want = len - pos;
            int read = in.read(buf, pos, want);
            if (read < 0) {
                return pos;
            }
            pos += read;
        }
        return pos;
    }

    private static final File[] file_empty = new File[0];

    public static Map<String, File> listFile(File dir, FileFilter filter) {
        File[] list = filter == null ? dir.listFiles() : dir.listFiles(filter);
        if (list == null) {
            list = file_empty;
        }
        Map<String, File> map = new HashMap<String, File>(list.length);
        for (File f : list) {
            map.put(f.getName(), f);
        }
        return map;
    }

    public static void closeJar(URLClassLoader loader, boolean ensureOpen) throws Exception {
        Object ucpObj = null;
        Field ucpField = URLClassLoader.class.getDeclaredField("ucp");
        ucpField.setAccessible(true);
        ucpObj = ucpField.get(loader);
        URL[] list = loader.getURLs();
        for (int i = 0; i < list.length; i++) {
            // 获得ucp内部的jarLoader
            Method m = ucpObj.getClass().getDeclaredMethod("getLoader", int.class);
            m.setAccessible(true);
            Object jarLoader = m.invoke(ucpObj, i);
            String clsName = jarLoader.getClass().getName();
            if (clsName.indexOf("JarLoader") != -1) {
                if (ensureOpen) {
                    m = jarLoader.getClass().getDeclaredMethod("ensureOpen");
                    m.setAccessible(true);
                    m.invoke(jarLoader);
                }
                m = jarLoader.getClass().getDeclaredMethod("getJarFile");
                m.setAccessible(true);
                JarFile jf = (JarFile) m.invoke(jarLoader);
                // 释放jarLoader中的jar文件
                jf.close();
            }
        }

    }

}
