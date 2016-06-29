package com.hiido.hcat.common.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class ErrCacheOutputStream extends OutputStream {
	private static final Charset utf8 = Charset.forName("UTF-8");
	
	StringBuilder builder = new StringBuilder();
	ByteBuffer buffer = ByteBuffer.allocate(512);
	
	@Override
	public void write(int b) throws IOException {
		if(buffer.position() >= buffer.capacity()) {
			buffer.flip();
			builder.append(utf8.decode(buffer));
			buffer.clear();
		}
		buffer.put((byte)b);
	}
	
	public String returnAndClear() {
		buffer.flip();
		builder.append(utf8.decode(buffer));
		return builder.toString();
	}

}
