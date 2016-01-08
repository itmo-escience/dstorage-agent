package itmo.escience.dstorage.agent.responses;

import itmo.escience.dstorage.agent.requests.AgentRequestType;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

/**
 *
 * @author anton
 */
public abstract class AgentResponse {
    private AgentRequestType type;
    //private ContentType contentType=ContentType.APPLICATION_JSON;
    protected ContentType contentType=ContentType.create("application/json", "UTF-8");
    protected int code=HttpStatus.SC_OK;
    private AgentMessage message;
    protected String jsonMsg="";
    
    public void setStatus(int status){this.code=status;}
    public int getStatus(){return code;}
    public void setType(AgentRequestType type){this.type=type;}
    public void setMessage(AgentMessage s){this.message=s;} 
    public void setJsonMsg(String s){this.jsonMsg=s;} 
    public String getMessage(){return jsonMsg;} 
    public void convert(HttpResponse response){
        //contentType.withCharset(Charset)
        response.setStatusCode(code);
        StringEntity entity=null;
        //try {
            entity = new StringEntity(message.getString(),contentType);
        //} catch (UnsupportedEncodingException ex) {
        //    Logger.getLogger(AgentResponse.class.getName()).log(Level.SEVERE, null, ex);
        //}
        //entity.setContentType("application/json; charset=UTF-8");
        response.setEntity(entity);
    }
}
