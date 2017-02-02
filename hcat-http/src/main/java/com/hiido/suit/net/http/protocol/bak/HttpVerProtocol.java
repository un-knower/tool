/**
 * Aug 10, 2012
 */
package com.hiido.suit.net.http.protocol.bak;

/**
 * @author lin
 * 
 */
public abstract class HttpVerProtocol extends HttpRawProtocol {

    private static final long serialVersionUID = 1L;
    protected int version = -1;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean matchVersion(int version) {
        return this.version == version;
    }
}
