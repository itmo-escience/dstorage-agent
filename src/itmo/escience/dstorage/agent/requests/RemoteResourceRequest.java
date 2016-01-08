package itmo.escience.dstorage.agent.requests;

import itmo.escience.dstorage.agent.responses.AgentResponse;
import org.apache.http.HttpRequest;

/**
 *
 * @author anton
 */
public class RemoteResourceRequest extends AgentRequest{
    private String user;
    private String pwd;

    RemoteResourceRequest(HttpRequest request){
        super(request);
    }
    public boolean isValid(){        
        return true;
    }
    
    @Override
    public AgentRequestType getType() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public AgentResponse process() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
