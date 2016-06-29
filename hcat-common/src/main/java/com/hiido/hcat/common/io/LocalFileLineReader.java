package com.hiido.hcat.common.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import com.hiido.hcat.common.util.IOUtils;

public class LocalFileLineReader extends AbstractLineReader {

    private static final String KEY_HDFS_BUF_SIZE = "io.file.buffer.size";

    private volatile String file;

    public LocalFileLineReader(String file, String charset) {
        super(charset);
        this.file = file;
    }

    public LocalFileLineReader(String file) {
        this.file = file;
    }

    @Override
    public void open() throws IOException {

        Path hdfsPath = new Path(file);
        Configuration conf = new Configuration();
        conf.setInt(KEY_HDFS_BUF_SIZE, bufSize);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(hdfsPath);
        FileInputStream inputStream = new FileInputStream(new File(file));
        try {
            if (codec == null) {
                reader = new BufferedReader(new InputStreamReader(inputStream, charset), bufSize);
            } else {
                CompressionInputStream comInputStream = codec.createInputStream(inputStream);
                reader = new BufferedReader(new InputStreamReader(comInputStream, charset), bufSize);
            }
        } catch (IOException e) {
            IOUtils.closeIO(inputStream);
            throw e;
        }

    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public BufferedReader getReader() {
        return this.reader;
    }

    public static void main(String[] args) {
        String file = "/home/cuizhijing/tmp/a.log.gz";
        LocalFileLineReader reader = new LocalFileLineReader(file);
        try {
            reader.open();
            String line = reader.getReader().readLine();
            System.out.println(line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
