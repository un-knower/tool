// Licensed to Cloudera, Inc. under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  Cloudera, Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.hiido.hcat.serivce.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

public class SeqLineBuffer {
	private static final String lineSeparator = (String) java.security.AccessController
			.doPrivileged(new sun.security.action.GetPropertyAction(
					"line.separator"));
	private final SortedMap<Integer, String> lines = new TreeMap<Integer, String>();
	private final int capacity;
	private int size;
	private int count;
	private final Filter filter;

	private String tmpData;

	public SeqLineBuffer(Filter f, int capacity) {
		this.capacity = capacity;
		this.filter = f;
	}

	public SeqLineBuffer(int capacity) {
		this(null, capacity);
	}

	public synchronized String lastLine() {
		Integer lastKey = lines.lastKey();
		return lastKey != null ? lines.get(lastKey) : null;
	}

	public synchronized int size() {
		return size;
	}

	public synchronized boolean write(String data) {
		if (lineSeparator.equals(data)) {
			if (tmpData != null) {
				tmpData+=data;
				if (filter != null)
					tmpData = filter.accept(tmpData, count);
				if(tmpData!=null){
					lines.put(count++, tmpData);
					size+=tmpData.length();
				}
				tmpData=null;
			}
			return true;
		}
		boolean end = data.endsWith(lineSeparator);
		List<String> list = new ArrayList<String>();
		for(String ss : data.split(lineSeparator)){
			list.add(ss);
		}
		
		int len = 0;
		int lastIndex = end ? list.size():list.size()-1;
		int i = 0;
		for (; i < lastIndex; i++) {
			if(i==0&&tmpData!=null){
				tmpData+=list.get(i);
				if (filter != null)
					tmpData = filter.accept(tmpData, count);
				if(tmpData!=null){
					lines.put(count++, tmpData+"\n");
					len+=tmpData.length();
				}
				tmpData=null;
				continue;
			}
			String line = null;
			if (filter != null) {
				line = filter.accept(list.get(i), count);
			}
			if (line == null || line.equals("")) {
                continue;
            }
			len += line.length()+1;
			lines.put(count++, line+"\n");
		}
		if(!end)
			tmpData=(tmpData==null)?list.get(i):tmpData+list.get(i);

		size += len;

		if (size > capacity) {
			Iterator<Integer> it = lines.keySet().iterator();
			while (size > capacity && it.hasNext()) {
				Integer k = it.next();
				String evicted = lines.get(k);
				if (evicted != null) {
					size -= evicted.length();
				}
				it.remove();
			}
		}
		return true;
	}

	public synchronized int count() {
		return count;
	}

	public String read(boolean containSeq, int begin) {
		StringBuilder sb = new StringBuilder();
		read(sb, containSeq, begin);
		return sb.toString();
	}

	public synchronized int read(StringBuilder sb, boolean containSeq, int begin) {
		Map<Integer, String> map = lines.tailMap(begin);
		if (!containSeq) {
			for (String s : map.values()) {
				sb.append(s);
			}
		} else {
			for (Entry<Integer, String> e : map.entrySet()) {
				String line = e.getValue();
				if (!line.endsWith("\n")) {
					line += "\n";
				}
				sb.append(line);
			}
		}
		return count;
	}

	public synchronized int read(StringBuilder sb, boolean containSeq,
			int lastCount, int begin) {
		if (lastCount != count) {
			return read(sb, containSeq, begin);
		}
		return 0;
	}

	public synchronized void clear() {
		lines.clear();
		size = 0;
	}

	public interface Filter {
		String accept(String line, int count);
	}

	public static final class OutStream extends OutputStream {
		private static final Charset utf8 = Charset.forName("UTF-8");
		final SeqLineBuffer buf;

		public OutStream(int capacity) {
			this.buf = new SeqLineBuffer(capacity);
		}

		public OutStream(Filter f, int capacity) {
			this.buf = new SeqLineBuffer(f, capacity);
		}

		@Override
		public void write(byte[] b) throws IOException {
			buf.write(new String(b, utf8));
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			buf.write(new String(b, off, len, utf8));
		}

		@Override
		public void write(int b) throws IOException {
			byte[] buf = { (byte) b };
			this.write(buf);
		}

		public SeqLineBuffer getBuffer() {
			return buf;
		}
	}

	public static void main(String[] args) throws Exception {
		// String a = "Task failed!\nTask ID:\n   Stage-5\n\nLogs:\n";
		// System.out.println(a);
		//
		SeqLineBuffer buf = new SeqLineBuffer(4);
		buf.write("a");
		buf.write("a");
		buf.write("aaaaaa");
		// buf.write(a);
		// buf.write("\n");
		// buf.write("aaa");
		// StringBuilder sb = new StringBuilder();
		// buf.read(sb, true, 0);
		// System.out.println(sb.toString());
		//
		// String[] lines = sb.toString().split("\n");
		// int lastNum = 0;
		// for (String line : lines) {
		// int idx = line.indexOf(':');
		// if (idx == -1) {
		// throw new
		// IllegalArgumentException(String.format("line not contain ':' [%s]",
		// line));
		// }
		// lastNum = Integer.valueOf(line.substring(0, idx));
		// if (idx + 1 == line.length()) {
		// continue;
		// }
		// sb.append(line.substring(idx + 1, line.length())).append("\n");
		// }
		Map<Integer, String> map = new HashMap<Integer, String>();
		map.put(1, "1");
		map.put(2, "2");
		Iterator<Integer> it = map.keySet().iterator();
		while (it.hasNext()) {
			Integer k = it.next();
			map.get(k);
			it.remove();
		}
	}
}
