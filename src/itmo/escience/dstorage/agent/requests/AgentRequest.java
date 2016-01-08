package itmo.escience.dstorage.agent.requests;

import itmo.escience.dstorage.agent.Agent;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.entity.ContentType;

/**
 *
 * @author anton
 */
public abstract class AgentRequest {
    private Map<String,String> headers=new HashMap();
    private String target;
    private ContentType contentType=ContentType.APPLICATION_OCTET_STREAM;
    private AgentMessage message;
    
    public AgentRequest(HttpRequest httpRequest){
        Header[] httpHeaders=httpRequest.getAllHeaders();
        for(Header header:httpHeaders)
            headers.put(header.getName(),header.getValue());
        try {
            target=URLDecoder.decode(httpRequest.getRequestLine().getUri(),Agent.getLocalEncoding());
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(AgentRequest.class.getName()).log(Level.SEVERE, null, ex);
        }       
        initContentType();
    }
    public boolean checkHeaders(String[] listHeaders){
        for(String s:listHeaders)
            if(!headers.containsKey(s)) return false;
        return true; //все заголовки заданного типа есть
    }
    
    public String getMessage(){return message.getString();}
    public String getHeader(String headerName){return headers.get(headerName);}
    public String getTarget(){return this.target;}
    public void initContentType(){
        if(target.contains("?")){//there is contentType inside request from client, keep them and return entity with
            contentType=ContentType.parse((target.split("/?", 2)[1]).split("=", 2)[1]);
            Agent.log.info("CT detected = " +contentType.toString());
            //keep only name of file in target variable
            target=target.split("\\?",2)[0];
        }
    }
    public ContentType getContentType(){return contentType;}
    public abstract AgentRequestType getType();
    public abstract AgentResponse process();    
}

