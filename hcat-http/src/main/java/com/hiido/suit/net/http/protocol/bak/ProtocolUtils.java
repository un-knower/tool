/**
 * May 31, 2013
 */
package com.hiido.suit.net.http.protocol.bak;

/**
 * @author lin
 * 
 */
public final class ProtocolUtils {
    private ProtocolUtils() {
    }

    public static boolean isVersionMatch(HttpVerProtocol pro, String from, RetProtocolReply reply, int ver) {
        if (!pro.matchVersion(ver)) {
            String msg = String.format("protocol_srv ver=%d,but protocol_cli[%s] ver=%d", ver, from, pro.getVersion());
            reply.mismatchVersion(msg);
            return false;
        }
        return true;
    }
}
