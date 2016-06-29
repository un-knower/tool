/**
 * Oct 16, 2012
 */
package com.hiido.suit.net.http.protocol;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.log4j.Logger;

import com.hiido.hcat.common.PublicConstant;
import com.hiido.hcat.common.err.ErrCode;
import com.hiido.hcat.common.err.ErrCodeException;
import com.hiido.hcat.common.util.StringUtils;

public final class HttpApacheClient extends AbstractHttpProtocolClient implements Cloneable{
    private static final Map<Integer, ErrCode> errMap = new HashMap<Integer, ErrCode>();
    static {
        errMap.put(PublicConstant.HTTP_SOFT_REJECT, ErrCode.RedirectErr);
    }
    private final HttpClient client;
    private final HttpConnectionManagerParams managerParams;

    private final Object sync = new Object();

    private static final Logger log = Logger.getLogger(HttpApacheClient.class);

    public HttpApacheClient() {
        this(DEF_CONN_TIMEOUT, DEF_READ_TIMEOUT);
    }

    public HttpApacheClient(int connTime, int readTime) {
        super(connTime, readTime);
        this.client = new HttpClient();
        managerParams = client.getHttpConnectionManager().getParams();
    }

    private PostMethod init(String url) {
        PostMethod m = new PostMethod(url);
        managerParams.setConnectionTimeout(connTimeout);
        managerParams.setSoTimeout(readTimeout);
        m.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");
        if (keepAlive) {
            m.setRequestHeader("Connection", "keep-alive");
        } else {
            m.setRequestHeader("Connection", "close");
        }
        return m;
    }

    private String doMethod(String url, HttpMethodBase m) throws ErrCodeException {
        boolean sucess = false;
        try {
            int statusCode = client.executeMethod(m);
            if (log.isDebugEnabled()) {
                Header[] header = m.getResponseHeaders();
                for (Header h : header) {
                    log.debug(h.toString());
                }
            }
            String reponse = m.getResponseBodyAsString();

            if (statusCode != HttpStatus.SC_OK) {
                String errMsg = String.format("http[%s] httpcode=%d,msg=%s", url, statusCode, reponse);
                ErrCode err = errMap.get(statusCode);
                if (err != null) {
                    throw new ErrCodeException(err, errMsg);
                }
                throw ErrCodeException.remoteErr(errMsg);
            }
            sucess = true;
            return reponse;
        } catch (java.net.SocketTimeoutException e) {
            throw ErrCodeException.timeoutErr("timeout in:" + url, e);
        } catch (ConnectException e) {
            throw ErrCodeException.ioErr("connect err:" + url, e);
        } catch (IOException e) {
            throw ErrCodeException.ioErr("failed to:" + url, e);
        } catch (ErrCodeException e) {
            throw e;
        } catch (Exception e) {
            throw ErrCodeException.runErr("failed to:" + url, e);
        } finally {
            if (sucess) {
                m.releaseConnection();
            } else {
                close();
            }
        }
    }

    private String doPostString(String url, String content, PostMethod m) throws ErrCodeException {
        try {
            m.setRequestEntity(new StringRequestEntity(content, CONTENT_TYPE, CONTENT_CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return doMethod(url, m);

    }

    @Override
    public <T extends ProtocolReply> T post(String url, HttpProtocol protocol, Class<T> clazz) throws ErrCodeException {
        String json = protocol.toJsonString();
        String replyStr = null;
        synchronized (sync) {
            PostMethod m = init(url);
            replyStr = doPostString(url, json, m);
        }
        T reply = null;
        if (replyStr != null) {
            try {
                reply = StringUtils.jsonFromString(replyStr, clazz);
            } catch (Exception e) {
                throw ErrCodeException.runErr(
                        String.format("failed to parse %s from json=%s", clazz.getName(), replyStr), e);
            }
            reply.checkSucess();
        }
        return reply;
    }

    @Override
    public <T extends ProtocolReply> T post(String host, int port, HttpProtocol protocol, Class<T> clazz)
            throws ErrCodeException {
        String url = createUrl("http", host, port, protocol.name());
        return post(url, protocol, clazz);
    }

    @Override
    public <T extends ProtocolReply> T postSSL(String host, int port, HttpProtocol protocol, Class<T> clazz)
            throws ErrCodeException {
        String url = createUrl("https", host, port, protocol.name());
        return post(url, protocol, clazz);
    }

    @Override
    public String post(String host, int port, String path, String content) throws ErrCodeException {
        String url = createUrl("http", host, port, path);
        synchronized (sync) {
            PostMethod m = init(url);
            return doPostString(url, content, m);
        }
    }

    @Override
    public String postSSL(String host, int port, String path, String content) throws ErrCodeException {
        String url = createUrl("https", host, port, path);
        synchronized (sync) {
            PostMethod m = init(url);
            return doPostString(url, content, m);
        }
    }

    @Override
    public void close() {
        synchronized (sync) {
            client.getHttpConnectionManager().closeIdleConnections(0);
        }
    }

    @Override
    public String post(String url, String content) throws ErrCodeException {
        final long start = System.currentTimeMillis();
        long syncStart = start;
        String result = null;
        try {
            synchronized (sync) {
                syncStart = System.currentTimeMillis();
                PostMethod m = init(url);
                result = doPostString(url, content, m);
            }
        } finally{
            long end = System.currentTimeMillis();
            if(log.isDebugEnabled()){
                log.debug(String.format("url[%s],content[%s],spent[%d],sync[%d]",url,content,end-start,syncStart-start));
            }
        }

        return result;        
    }

    @Override
    public String get(String url) throws ErrCodeException {
        GetMethod m = new GetMethod(url);
        synchronized (sync) {
            managerParams.setConnectionTimeout(connTimeout);
            managerParams.setSoTimeout(readTimeout);
            m.getParams().setParameter(HttpMethodParams.HTTP_CONTENT_CHARSET, "utf-8");
            if (keepAlive) {
                m.setRequestHeader("Connection", "keep-alive");
            } else {
                m.setRequestHeader("Connection", "close");
            }
            return doMethod(url, m);
        }
    }
    public void setRetry(int retryTimes){
    	client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
    	new DefaultHttpMethodRetryHandler(retryTimes, false));
    }
    @Override
    public HttpApacheClient clone(){
        HttpApacheClient apache = new HttpApacheClient(connTimeout, readTimeout);
        apache.setKeepAlive(keepAlive);
        return apache;
    }

	@Override
	public <T extends ProtocolReply> T post(String host, String clientAddr,
			int port, HttpProtocol protocol, Class<T> clazz)
			throws ErrCodeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends ProtocolReply> T post(String url, String clientAddr,
			HttpProtocol protocol, Class<T> clazz) throws ErrCodeException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T extends ProtocolReply> T postForWeb(String host,
			String clientAddr, int port, HttpProtocol protocol, Class<T> clazz)
			throws ErrCodeException {
		// TODO Auto-generated method stub
		return null;
	}

}
