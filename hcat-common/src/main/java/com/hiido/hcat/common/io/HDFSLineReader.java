package com.hiido.hcat.common.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import com.hiido.hcat.common.util.IOUtils;

public final class HDFSLineReader extends AbstractLineReader {
    private static final String KEY_HDFS_BUF_SIZE = "io.file.buffer.size";

    private String file;

    public HDFSLineReader(String file, String charset) {
        super(charset);
        this.file = file;
    }

    public HDFSLineReader() {
    }

    @Override
    public void open() throws IOException {
        Path hdfsPath = new Path(file);
        Configuration conf = new Configuration();
        conf.setInt(KEY_HDFS_BUF_SIZE, bufSize);
        FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(hdfsPath);

        FSDataInputStream inputStream = fs.open(hdfsPath);

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

}
