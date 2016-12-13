package com.hiido.hcat.databus;

import com.hiido.hcat.thrift.protocol.*;
import com.hiido.hcat.thrift.protocol.RuntimeException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zrc on 16-12-13.
 */
public class HcatDatabusServer implements CliService.Iface, SignupService.Iface {

    private static final Logger LOG = LoggerFactory.getLogger(HcatDatabusServer.class);

    private FetchScheduler fetchScheduler;

    public void start() {

    }


    public class QueryServlet extends TServlet {

        public QueryServlet(TProcessor processor, TProtocolFactory protocolFactory) {
            super(processor, protocolFactory);
        }
    }

    @Override
    public CommitQueryReply commit(CommitQuery cq) throws AuthorizationException, RuntimeException, TException {

        return null;
    }

    @Override
    public QueryStatusReply queryJobStatus(QueryStatus qs) throws NotFoundException, TException {
        return null;
    }

    @Override
    public CancelQueryReply cancelJob(CancelQuery cq) throws AuthorizationException, NotFoundException, TException {
        throw new AuthorizationException("Unsupport cancelJob.");
    }

    @Override
    public LoadFileReply laodData(LoadFile lf) throws AuthorizationException, RuntimeException, TException {
        throw new AuthorizationException("Unsupport loadData.");
    }

    @Override
    public SignupReply signup(int companyId, String companyName, int userId) throws AuthorizationException, RuntimeException, TException {
        throw new AuthorizationException("Unsupport signup.");
    }
}
