package com.hiido.hcat.cli;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.hcat.thrift.protocol.CliService;

import jline.console.ConsoleReader;

public class CliDriver {
	
	public static final String HIVE_L4J = "hivecli-log4j.properties";
	public static String prompt = "hive";
    public static String prompt2 = "    "; // when ';' is not yet seen
    public static final int LINES_TO_FETCH = 40; // number of lines to fetch in
                                                 // batch from remote hive
                                                 // server

    public static final String HIVERCFILE = ".hiverc";
	
    private Configuration conf;
    private final LogHelper console;
	private CliService.Client client;
	protected ConsoleReader reader;
	private final String originalThreadName;
	
	
	public CliDriver(String url) {
		SessionState ss = SessionState.get();
	    conf = (ss != null) ? ss.getConf() : new Configuration();
	    Logger LOG = LoggerFactory.getLogger("CliDriver");
	    if (LOG.isDebugEnabled()) {
	      LOG.debug("CliDriver inited with classpath {}", System.getProperty("java.class.path"));
	    }
	    console = new LogHelper(LOG);
	    originalThreadName = Thread.currentThread().getName();
		try {
			THttpClient thc = new THttpClient(url);
			TProtocol lopFactory = new TBinaryProtocol(thc);
			client = new CliService.Client(lopFactory);
		} catch (TTransportException e) {
			System.err.println("failed to create ThttpClient :" + e.toString());
			System.exit(1);
		}
	}
	
	private int executeDriver(CliSessionState ss, HiveConf conf) {
		String line;
		int ret = 0;
		String prefix = "";
		String curDB = getForamttedDb(conf, ss);
		//TODO
		return 0;
	}
	
	public int processCmd(String cmd) {
		String cmd_trimmed = cmd.trim();
	    String[] tokens = tokenizeCmd(cmd_trimmed);
	    int ret = 0;

	    if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {
	    	System.exit(0);
	    }
	    return 0;
	}
	
	  private static String getForamttedDb(HiveConf conf, CliSessionState ss) {
		    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.CLIPRINTCURRENTDB)) {
		      return "";
		    }
		    //BUG: This will not work in remote mode - HIVE-5153
		    String currDb = SessionState.get().getCurrentDatabase();

		    if (currDb == null) {
		      return "";
		    }

		    return " (" + currDb + ")";
		  }
	
	
	private String[] tokenizeCmd(String cmd) {
	    return cmd.split("\\s+");
	  }
	
	public static void main(String[] args) {
		
	}
	
}
