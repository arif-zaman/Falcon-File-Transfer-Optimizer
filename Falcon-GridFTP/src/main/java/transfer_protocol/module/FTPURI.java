package transfer_protocol.module;

import org.ietf.jgss.GSSCredential;

import java.net.URI;

// Wraps a URI and a credential into one object and makes sure the URI
// represents a supported protocol. Also parses out a bunch of stuff.
public class FTPURI {
    @SuppressWarnings("unused")
    public final URI uri;
    public final GSSCredential cred;

    public final boolean gridftp, ftp, file;
    public final String host, proto;
    public final int port;
    public final String user, pass;
    public final String path;

    public FTPURI(URI uri, GSSCredential cred) throws Exception {
        this.uri = uri;
        this.cred = cred;
        host = uri.getHost();
        proto = uri.getScheme();
        int p = uri.getPort();
        String ui = uri.getUserInfo();

        if (uri.getPath().startsWith("/~")) {
            path = uri.getPath().substring(1);
        } else {
            path = uri.getPath();
        }

        // Check protocol and determine port.
        if (proto == null || proto.isEmpty()) {
            throw new Exception("no protocol specified");
        }
        if ("gridftp".equals(proto) || "gsiftp".equals(proto)) {
            port = (p > 0) ? p : 2811;
            gridftp = true;
            ftp = false;
            file = false;
        } else if ("ftp".equals(proto)) {
            port = (p > 0) ? p : 21;
            gridftp = false;
            ftp = true;
            file = false;
        } else if ("file".equals(proto)) {
            port = -1;
            gridftp = false;
            ftp = false;
            file = true;
        } else {
            throw new Exception("unsupported protocol: " + proto);
        }

        // Determine username and password.
        if (ui != null && !ui.isEmpty()) {
            int i = ui.indexOf(':');
            user = (i < 0) ? ui : ui.substring(0, i);
            pass = (i < 0) ? "" : ui.substring(i + 1);
        } else {
            user = pass = null;
        }
    }
}
