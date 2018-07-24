package org.apache.hadoop.hive.ql.exec.spark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.serializer.KryoRegistrator;

import java.util.Arrays;

/**
 * see HIVE-15104
 * Add this class and HiveKey.class into spark-assembly-xx.jar
 * Created by zrc on 17-9-18.
 */
public class HiveKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(HiveKey.class, new HiveKeySerializer());
        kryo.register(BytesWritable.class, new BytesWritableSerializer());
    }

    private static class HiveKeySerializer extends Serializer<HiveKey> {

        @Override
        public void write(Kryo kryo, Output output, HiveKey object) {
            output.writeInt(object.getLength(), true);
            output.write(object.getBytes(), 0, object.getLength());
            output.writeInt(object.hashCode(), false);
        }

        @Override
        public HiveKey read(Kryo kryo, Input input, Class<HiveKey> type) {
            int len = input.readInt(true);
            byte[] bytes = new byte[len];
            input.readBytes(bytes);
            return new HiveKey(bytes, input.readInt(false));
        }
    }

    private static class BytesWritableSerializer extends Serializer<BytesWritable> {
        public void write(Kryo kryo, Output output, BytesWritable object) {
            output.writeInt(object.getLength(), true);
            output.write(object.getBytes(), 0, object.getLength());
        }

        public BytesWritable read(Kryo kryo, Input input, Class<BytesWritable> type) {
            int len = input.readInt(true);
            byte[] bytes = new byte[len];
            input.readBytes(bytes);
            return new BytesWritable(bytes);
        }
    }
}
