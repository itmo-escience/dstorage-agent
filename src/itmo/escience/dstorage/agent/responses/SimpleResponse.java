package itmo.escience.dstorage.agent.responses;

import itmo.escience.dstorage.agent.requests.AgentRequestType;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;

/**
 *
 * @author anton
 */
public class SimpleResponse extends AgentResponse{
    
    
    @Override
    public void convert(HttpResponse response) {
        response.setStatusCode(code);
        if(!this.jsonMsg.equals("")){
            StringEntity entity=null;
            entity = new StringEntity(jsonMsg,contentType);
            response.setEntity(entity);
        }
    }
    
}
