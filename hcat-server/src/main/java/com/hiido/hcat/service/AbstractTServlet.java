package com.hiido.hcat.service;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;

public abstract class AbstractTServlet extends TServlet{

	public AbstractTServlet(TProcessor processor, TProtocolFactory protocolFactory) {
		super(processor, protocolFactory);
	}

}
