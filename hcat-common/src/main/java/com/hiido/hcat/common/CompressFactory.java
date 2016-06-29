/**
 * Nov 19, 2013
 */
package com.hiido.hcat.common;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.hadoop.compression.lzo.LzopCodec;
import com.hiido.hadoop.io.compress.gzp.GzippCodec;
import com.hiido.hadoop.io.compress.snyp.SnypCodec;
import com.hiido.hcat.common.util.IOUtils;
import com.hiido.hcat.common.util.StringUtils;

public final class CompressFactory implements PublicConstant {
    private static final Set<String> index = new HashSet<String>();
    static {
        index.add("lzo");
        index.add("snyp");
        index.add("gzp");
    }
    private final Configuration conf;
    private final CompressionCodecFactory fc;

    public CompressFactory(Configuration conf) {
        this.conf = conf;
        this.fc = new CompressionCodecFactory(conf);
    }

    public boolean isCompress(String name) {
        Path p = new Path(name);
        return isCompress(p);
    }

    public boolean isCompress(Path p) {
        return fc.getCodec(p) != null;
    }

    public boolean autoIndex() {
        String comp = conf.get(HCAT_LOAD_COMPRESS);
        if (StringUtils.isEmpty(comp) || comp.equalsIgnoreCase("non")) {
            return false;
        }
        boolean auto = conf.getBoolean(HCAT_COMPRESS_INDEX_AUTO, true);
        return auto && index.contains(comp);
    }

    public CompressionCodec getCodec() throws IOException {
        String comp = conf.get(HCAT_LOAD_COMPRESS);
        if (StringUtils.isEmpty(comp)) {
            return null;
        }

        Path p = new Path("/tmp/test." + comp);
        CompressionCodec codec = fc.getCodec(p);
        if (codec == null) {
            throw new IOException(String.format("unsupported %s=%s", HCAT_LOAD_COMPRESS, comp));
        }
        return codec;
    }

    public Path createCompressName(Path parent, String name, CompressionCodec codec) {
        if (codec == null) {
            return new Path(parent, name);
        }
        String suffix = codec.getDefaultExtension();
        return new Path(parent, name + suffix);
    }

    public OutputStream wrap(Path p, FileSystem fs, boolean overwriteIdx, CompressionCodec codec, boolean index,
            OutputStream out, Configuration conf, long filesize) throws IOException {
        if (codec == null) {
            return null;
        }
        if (index) {
            Path idx = new Path(p.getParent(), p.getName() + ".index");
            long min = conf.getLong(PublicConstant.HCAT_COMPRESS_INDEX_MIN_SIZE,
                    PublicConstant.DEF_HCAT_COMPRESS_INDEX_MIN_SIZE);
            DataOutputStream indexOut = null;
            try {
                if (codec instanceof LzopCodec) {
                    indexOut = filesize < min * 2 ? null : fs.create(idx, overwriteIdx);
                    return ((LzopCodec) codec).createIndexedOutputStream(out, indexOut);
                } else if (codec instanceof SnypCodec) {
                    indexOut = filesize < min * 2 ? null : fs.create(idx, overwriteIdx);
                    return ((SnypCodec) codec).createIndexedOutputStream(out, indexOut);
                } else if (codec instanceof GzippCodec) {
                    indexOut = filesize < min * 4 ? null : fs.create(idx, overwriteIdx);
                    return ((GzippCodec) codec).createIndexedOutputStream(out, indexOut);
                }
            } catch (Exception e) {
                IOUtils.closeIO(indexOut);
                throw new IOException(e);
            }
        }
        return codec.createOutputStream(out);
    }
}
