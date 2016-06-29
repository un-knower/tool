package com.hiido.hcat.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.util.Shell;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ContextHandler.Context;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HttpServer {

	private final String appDir;
	private final int port;
	private ServletContextHandler context;
	private final Server webServer;
	private IPAccessHandler ipAccessHandler;
	
	public HttpServer(Builder builder) throws IOException {
		this.port = builder.port;

		webServer = new Server();
		QueuedThreadPool pool = new QueuedThreadPool(builder.maxThreads);
		pool.setMinThreads(builder.minThreads);
		webServer.setThreadPool(pool);
		if(ipAccessHandler != null)
			webServer.setHandler(ipAccessHandler);
		appDir = getWebAppsPath(builder.name);
		initializeWebServer(builder);
	}

	public static class Builder {
		private String name;
		public String getName() {
			return name;
		}

		public Builder setName(String name) {
			this.name = name;
			return this;
		}

		public String getHost() {
			return host;
		}

		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		public int getPort() {
			return port;
		}

		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		public int getMinThreads() {
			return minThreads;
		}

		public Builder setMinThreads(int minThreads) {
			this.minThreads = minThreads;
			return this;
		}

		public int getMaxThreads() {
			return maxThreads;
		}

		public Builder setMaxThreads(int maxThreads) {
			this.maxThreads = maxThreads;
			return this;
		}

		public HiveConf getConf() {
			return conf;
		}

		public Builder setConf(HiveConf conf) {
			this.conf = conf;
			return this;
		}

		public Map<String, Object> getContextAttrs() {
			return contextAttrs;
		}

		public Builder setContextAttrs(Map<String, Object> contextAttrs) {
			this.contextAttrs = contextAttrs;
			return this;
		}

		public String getKeyStorePassword() {
			return keyStorePassword;
		}

		public Builder setKeyStorePassword(String keyStorePassword) {
			this.keyStorePassword = keyStorePassword;
			return this;
		}

		public String getKeyStorePath() {
			return keyStorePath;
		}

		public Builder setKeyStorePath(String keyStorePath) {
			this.keyStorePath = keyStorePath;
			return this;
		}

		public boolean isUseSSL() {
			return useSSL;
		}

		public Builder setUseSSL(boolean useSSL) {
			this.useSSL = useSSL;
			return this;
		}

		private String host;
		private int port;
		private int minThreads;
		private int maxThreads;
		private HiveConf conf;
		private Map<String, Object> contextAttrs = new HashMap<String, Object>();
		private String keyStorePassword;
		private String keyStorePath;
		private boolean useSSL;

		public HttpServer build() throws IOException {
			return new HttpServer(this);
		}
	}
	
	public void start() throws Exception {
		webServer.start();
	}
	
	public void stop() throws Exception {
		webServer.stop();
	}
	
	public int getPort() {
		return port;
	}

	String getWebAppsPath(String appName) throws FileNotFoundException {
		String relativePath = "hive-webapps";
		URL url = getClass().getClassLoader().getResource(relativePath);
		if (url == null) {
			throw new FileNotFoundException(relativePath + " not found in CLASSPATH");
		}
		String urlString = url.toString();
		return urlString.substring(0, urlString.lastIndexOf('/'));
	}

	/**
	 * Create the web context for the application of specified name
	 */
	WebAppContext createWebAppContext(Builder b) {
		WebAppContext ctx = new WebAppContext();
		setContextAttributes(ctx.getServletContext(), b.contextAttrs);
		ctx.setDisplayName(b.name);
		ctx.setContextPath("/");
		ctx.setWar(appDir + "/" + b.name);
		return ctx;
	}

	/**
	 * Set servlet context attributes that can be used in jsp.
	 */
	void setContextAttributes(Context ctx, Map<String, Object> contextAttrs) {
		for (Map.Entry<String, Object> e : contextAttrs.entrySet()) {
			ctx.setAttribute(e.getKey(), e.getValue());
		}
	}

	void initializeWebServer(Builder b) {
		// Create the thread pool for the web server to handle HTTP requests
		QueuedThreadPool threadPool = new QueuedThreadPool();
		if (b.maxThreads > 0) {
			threadPool.setMaxThreads(b.maxThreads);
		}
		threadPool.setDaemon(true);
		threadPool.setName(b.name + "-web");
		webServer.setThreadPool(threadPool);

		Connector connector = createChannelConnector(threadPool.getMaxThreads(), b);
		connector.setHost(b.host);
		connector.setPort(port);
		webServer.addConnector(connector);

		context = new ServletContextHandler(
		          ServletContextHandler.SESSIONS);
		      context.setContextPath("/");
		webServer.setHandler(context);
	}

	/**
	 * Create a channel connector for "http/https" requests
	 */
	Connector createChannelConnector(int queueSize, Builder b) {
		SelectChannelConnector connector;
		if (!b.useSSL) {
			connector = new SelectChannelConnector();
		} else {
			SslContextFactory sslContextFactory = new SslContextFactory();
			sslContextFactory.setKeyStorePath(b.keyStorePath);
			Set<String> excludedSSLProtocols = Sets.newHashSet(Splitter.on(",").trimResults().omitEmptyStrings()
					.split(Strings.nullToEmpty(b.conf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST))));
			sslContextFactory
					.addExcludeProtocols(excludedSSLProtocols.toArray(new String[excludedSSLProtocols.size()]));
			sslContextFactory.setKeyStorePassword(b.keyStorePassword);
			connector = new SslSelectChannelConnector(sslContextFactory);
		}

		connector.setLowResourcesMaxIdleTime(10000);
		connector.setAcceptQueueSize(queueSize);
		connector.setResolveNames(false);
		connector.setUseDirectBuffers(false);
		connector.setReuseAddress(!Shell.WINDOWS);
		return connector;
	}

	/**
	 * Add a servlet in the server.
	 * 
	 * @param name
	 *            The name of the servlet (can be passed as null)
	 * @param pathSpec
	 *            The path spec for the servlet
	 * @param clazz
	 *            The servlet class
	 */
	void addServlet(String name, String pathSpec, Class<? extends HttpServlet> clazz) {
		ServletHolder holder = new ServletHolder(clazz);
		if (name != null) {
			holder.setName(name);
		}
		context.addServlet(holder, pathSpec);
	}
	
	void addServlet(String name, String pathSpec, HttpServlet servlet) {
	    ServletHolder holder = new ServletHolder(servlet);
	    if (name != null) {
            holder.setName(name);
        }
	    //webAppContext.addServlet(holder, pathSpec);
	    context.addServlet(new ServletHolder(servlet), pathSpec);
	}

	public IPAccessHandler getIpAccessHandler() {
		return ipAccessHandler;
	}

	public void setIpAccessHandler(IPAccessHandler ipAccessHandler) {
		this.ipAccessHandler = ipAccessHandler;
	}
	
}
