package itmo.escience.dstorage.agent.requests;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.utils.AgentAccessURI;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpRequest;
import org.apache.http.protocol.HttpContext;

/**
 *
 * @author anton
 */
public class RequestFactory {
    public static AgentRequest create(HttpRequest request){
        String method=request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
        AgentAccessURI uri=AgentAccessURI.EMPTY;                
        /*
        for(AgentAccessURI u:AgentAccessURI.values()){
            try {
                if(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals(u.getString()))
                    uri=AgentAccessURI.valueOf(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(RequestFactory.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        */
        try{
            uri=AgentAccessURI.parseUri(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(RequestFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
        //try {
        //    uri=AgentAccessURI.valueOf(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        //} catch (UnsupportedEncodingException ex) {
        //    Logger.getLogger(RequestFactory.class.getName()).log(Level.SEVERE, null, ex);
        //}
        switch (method){
            case "DELETE": return new CommandRequest(request);
            case "PUT":
            case "POST":                
                switch (uri){
                    case REMOTERESOURCE : return new RemoteResourceRequest(request);
                    case MAP: 
                    case ZIP: 
                    case REDUCE:
                    case EMPTY: return new UploadFileRequest(request); //just upload file to agent
                    default:    throw new RuntimeException("Unsupported URI inside method:"+method);
                }
            case "GET":
                switch (uri){
                    case AGENTSTATUS : 
                    case LVLMGMNT: return new CommandRequest(request);
                    case AGENTGET: return new CommandRequest(request);
                    case AGENTSTAT: 
                    case EMPTY: return new DownloadFileRequest(request); //just download file to agent
                    default:    throw new RuntimeException("Unsupported URI inside method:"+method);
                }
            default: throw new RuntimeException("Unsupported method type");
                
        }
    }
}
