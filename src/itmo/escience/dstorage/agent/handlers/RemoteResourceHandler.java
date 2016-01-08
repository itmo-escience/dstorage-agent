package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.AgentSystem;
import itmo.escience.dstorage.agent.Commands;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.RemoteResourceRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import java.net.URLDecoder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;

/**
 *
 * @author anton
 */
public class RemoteResourceHandler implements IRequestHandler{

    @Override
    public AgentResponse handle(AgentRequest request) {
        /*
        RemoteResourceRequest remoteRequest =(RemoteResourceRequest)request;
        if ((request instanceof HttpEntityEnclosingRequest) && 
                URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/remoteresource")) {
                        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();
                        String entityContent = EntityUtils.toString(enRequest);
                        Main.log.info("Incoming PUT Request:\n" + entityContent);
                        JSONObject jsonPutRequest = AgentSystem.parseJSON(entityContent);
                        Main.log.info("JSON incoming PUT Request on /remoteresource = " + jsonPutRequest);
                        JSONObject jsonStatus=new JSONObject();
                        //TO DO if not action generation error
                        if (jsonPutRequest.containsKey("action")){
                            jsonStatus=Commands.handleRRCommand(jsonPutRequest);
                            response.setStatusCode(HttpStatus.SC_OK);
                        } else {
                            jsonStatus.put("msg", "Bad request");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                        }    
                        Main.log.info(jsonStatus.toString()); 
                        StringEntity strEntRequest = new StringEntity (jsonStatus.toString(),System.getProperty("file.encoding"));
                        strEntRequest.setContentType("application/json");     
                        response.setEntity(strEntRequest);
                        return;     
                }
        */
        return new SimpleResponse();
    }
    
}
