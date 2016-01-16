package itmo.escience.dstorage.agent.responses;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NFileEntity;

/**
 *
 * @author anton
 */
public class DownloadFileResponse extends AgentResponse{
    private AbstractHttpEntity entity;
    
    
    
    public void setContentType(ContentType cType){contentType=cType;}
    public void setEntity(ByteArrayEntity entity){this.entity=entity;}
    public void setEntity(NFileEntity entity){this.entity=entity;}
    
    @Override
    public void convert(HttpResponse response) {
        response.setStatusCode(getStatus());
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS, DELETE");
        response.setEntity(this.entity);
    }
    
}
