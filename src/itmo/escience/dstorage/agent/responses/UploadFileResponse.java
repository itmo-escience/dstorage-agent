package itmo.escience.dstorage.agent.responses;

import itmo.escience.dstorage.agent.utils.AgentMessage;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;

/**
 *
 * @author anton
 */
public class UploadFileResponse extends AgentResponse {
    
    @Override
    public void convert(HttpResponse response) {
        response.setStatusCode(getStatus());
        StringEntity entity=null;
        try {
            entity = new StringEntity(getMessage());
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(UploadFileResponse.class.getName()).log(Level.SEVERE, null, ex);
        }
        entity.setContentType("application/json; charset=UTF-8");
        response.setEntity(entity);
    }
    
}
