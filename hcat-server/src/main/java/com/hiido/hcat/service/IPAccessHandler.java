package com.hiido.hcat.service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.PathMap;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.server.AbstractHttpConnection;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.HandlerWrapper;
import org.eclipse.jetty.util.IPAddressMap;

public class IPAccessHandler extends HandlerWrapper implements Handler {
	private static final Logger LOG = Logger.getLogger(IPAccessHandler.class);

	IPAddressMap<PathMap> _white = new IPAddressMap<PathMap>();
	IPAddressMap<PathMap> _black = new IPAddressMap<PathMap>();

	/* ------------------------------------------------------------ */
	/**
	 * Creates new handler object
	 */
	public IPAccessHandler() {
		super();
	}

	/* ------------------------------------------------------------ */
	/**
	 * Creates new handler object and initializes white- and black-list
	 * 
	 * @param white
	 *            array of whitelist entries
	 * @param black
	 *            array of blacklist entries
	 */
	public IPAccessHandler(String[] white, String[] black) {
		super();

		if (white != null && white.length > 0)
			setWhite(white);
		if (black != null && black.length > 0)
			setBlack(black);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Add a whitelist entry to an existing handler configuration
	 * 
	 * @param entry
	 *            new whitelist entry
	 */
	public void addWhite(String entry) {
		add(entry, _white);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Add a blacklist entry to an existing handler configuration
	 * 
	 * @param entry
	 *            new blacklist entry
	 */
	public void addBlack(String entry) {
		add(entry, _black);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Re-initialize the whitelist of existing handler object
	 * 
	 * @param entries
	 *            array of whitelist entries
	 */
	public void setWhite(String[] entries) {
		set(entries, _white);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Re-initialize the blacklist of existing handler object
	 * 
	 * @param entries
	 *            array of blacklist entries
	 */
	public void setBlack(String[] entries) {
		set(entries, _black);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Checks the incoming request against the whitelist and blacklist
	 * 
	 * @see org.eclipse.jetty.server.handler.HandlerWrapper#handle(java.lang.String,
	 *      org.eclipse.jetty.server.Request,
	 *      javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		AbstractHttpConnection connection = baseRequest.getConnection();
		if (connection != null) {
			EndPoint endp = connection.getEndPoint();
			if (endp != null) {
				String addr = endp.getRemoteAddr();
				if (addr != null && !isAddrUriAllowed(addr, request.getPathInfo())) {
					response.sendError(HttpStatus.FORBIDDEN_403);
					baseRequest.setHandled(true);
					return;
				}
			}
		}
		super.handle(target, baseRequest, request, response);
	}

	/* ------------------------------------------------------------ */
	/**
	 * Helper method to parse the new entry and add it to the specified address
	 * pattern map.
	 * 
	 * @param entry
	 *            new entry
	 * @param patternMap
	 *            target address pattern map
	 */
	protected void add(String entry, IPAddressMap<PathMap> patternMap) {
		if (entry != null && entry.length() > 0) {
			boolean deprecated = false;
			int idx;
			if (entry.indexOf('|') > 0) {
				idx = entry.indexOf('|');
			} else {
				idx = entry.indexOf('/');
				deprecated = (idx >= 0);
			}

			String addr = idx > 0 ? entry.substring(0, idx) : entry;
			String path = idx > 0 ? entry.substring(idx) : "/*";

			if (addr.endsWith("."))
				deprecated = true;
			if (path != null && (path.startsWith("|") || path.startsWith("/*.")))
				path = path.substring(1);

			PathMap pathMap = patternMap.get(addr);
			if (pathMap == null) {
				pathMap = new PathMap(true);
				patternMap.put(addr, pathMap);
			}
			if (path != null)
				pathMap.put(path, path);

			if (deprecated)
				LOG.debug(toString() + " - deprecated specification syntax: " + entry);
		}
	}

	/* ------------------------------------------------------------ */
	/**
	 * Helper method to process a list of new entries and replace the content of
	 * the specified address pattern map
	 * 
	 * @param entries
	 *            new entries
	 * @param patternMap
	 *            target address pattern map
	 */
	protected void set(String[] entries, IPAddressMap<PathMap> patternMap) {
		patternMap.clear();

		if (entries != null && entries.length > 0) {
			for (String addrPath : entries) {
				add(addrPath, patternMap);
			}
		}
	}

	/* ------------------------------------------------------------ */
	/**
	 * Check if specified request is allowed by current IPAccess rules.
	 * 
	 * @param addr
	 *            internet address
	 * @param path
	 *            context path
	 * @return true if request is allowed
	 *
	 */
	protected boolean isAddrUriAllowed(String addr, String path) {
		if (_white.size() > 0) {
			boolean match = false;

			Object whiteObj = _white.getLazyMatches(addr);
			if (whiteObj != null) {
				List whiteList = (whiteObj instanceof List) ? (List) whiteObj : Collections.singletonList(whiteObj);

				for (Object entry : whiteList) {
					PathMap pathMap = ((Map.Entry<String, PathMap>) entry).getValue();
					if (match = (pathMap != null && (pathMap.size() == 0 || pathMap.match(path) != null)))
						break;
				}
			}

			if (!match)
				return false;
		}

		if (_black.size() > 0) {
			Object blackObj = _black.getLazyMatches(addr);
			if (blackObj != null) {
				List blackList = (blackObj instanceof List) ? (List) blackObj : Collections.singletonList(blackObj);

				for (Object entry : blackList) {
					PathMap pathMap = ((Map.Entry<String, PathMap>) entry).getValue();
					if (pathMap != null && (pathMap.size() == 0 || pathMap.match(path) != null))
						return false;
				}
			}
		}

		return true;
	}

	/* ------------------------------------------------------------ */
	/**
	 * Dump the white- and black-list configurations when started
	 * 
	 * @see org.eclipse.jetty.server.handler.HandlerWrapper#doStart()
	 */
	@Override
	protected void doStart() throws Exception {
		super.doStart();

		if (LOG.isDebugEnabled()) {
			System.err.println(dump());
		}
	}

	/* ------------------------------------------------------------ */
	/**
	 * Dump the handler configuration
	 */
	public String dump() {
		StringBuilder buf = new StringBuilder();

		buf.append(toString());
		buf.append(" WHITELIST:\n");
		dump(buf, _white);
		buf.append(toString());
		buf.append(" BLACKLIST:\n");
		dump(buf, _black);

		return buf.toString();
	}

	/* ------------------------------------------------------------ */
	/**
	 * Dump a pattern map into a StringBuilder buffer
	 * 
	 * @param buf
	 *            buffer
	 * @param patternMap
	 *            pattern map to dump
	 */
	protected void dump(StringBuilder buf, IPAddressMap<PathMap> patternMap) {
		for (String addr : patternMap.keySet()) {
			for (Object path : ((PathMap) patternMap.get(addr)).values()) {
				buf.append("# ");
				buf.append(addr);
				buf.append("|");
				buf.append(path);
				buf.append("\n");
			}
		}
	}
}
