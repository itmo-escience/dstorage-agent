package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.StorageLayer;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.DownloadFileRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.DownloadFileResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import org.apache.http.HttpConnection;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HttpCoreContext;

/**
 *
 * @author anton
 */
public class DownloadFileHandler implements IRequestHandler {
    private DownloadFileRequest request;
    private DownloadFileResponse response;
    
    private boolean validateRequest(AgentRequest request){
        if(!(request instanceof DownloadFileRequest)) {
            response.setStatus(HttpStatus.SC_BAD_REQUEST);
            response.setMessage(AgentMessage.FAILED);
            return false;
        }
        this.request=(DownloadFileRequest)request;
        //bypass validation for file "/clientaccesspolicy.xml"
        if(this.request.getTarget().equals("/clientaccesspolicy.xml")) return true;
        
        if(!Ticket.validateStorageTicket(request)){
            response.setStatus(HttpStatus.SC_FORBIDDEN);
            response.setMessage(AgentMessage.FORBIDDEN);
            return false;
        }
        return true;
    }

    @Override
    public AgentResponse handle(AgentRequest agentRequest) {
        response=new DownloadFileResponse();
        if(!validateRequest(agentRequest))return response;
        StorageLayer layer=Main.getStorageLayer();
        
        String filename = this.request.getTarget();
        
        if (!layer.isFileExist(filename)) {
            response.setStatus(HttpStatus.SC_NOT_FOUND);
            response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("File "+filename +" isn't found", 
                    AgentSystemStatus.FAILED));     
            return response;
        }
        if (!layer.isFileAccessed(filename)) {
            response.setStatus(HttpStatus.SC_FORBIDDEN);
            response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("File "+filename +" isn't accessible", 
                    AgentSystemStatus.FAILED));
            return response;
        }
        //special handle for /clientaccesspolicy.xml file
        if(this.request.getTarget().equals("/clientaccesspolicy.xml")){ 
            response.setEntity(layer.getFile(this.request.getStorageLevel(),filename, ContentType.TEXT_HTML));
            response.setContentType(ContentType.TEXT_HTML);
        }
        else{
            //ByteArrayEntity bae=layer.getFile(this.request.getStorageLevel(),filename, this.request.getContentType());
            if(!layer.isFileOnLevel(this.request.getStorageLevel(), filename)){
                response.setStatus(HttpStatus.SC_NOT_FOUND);
                response.setEntity(new ByteArrayEntity((AgentMessageCreater.createJsonError(AgentMessage.NOTFOUND.getString(), AgentSystemStatus.FAILED)).getBytes(),
                        ContentType.APPLICATION_JSON));
            }
            else {
                //HttpCoreContext coreContext = HttpCoreContext.adapt(request.getContext());
                //HttpConnection conn = coreContext.getConnection(HttpConnection.class);
                if(layer.isFileOnLevel(StorageLevel.MEM, filename))
                    response.setEntity(layer.getFile(this.request.getStorageLevel(),filename, this.request.getContentType()));
                else
                    response.setEntity(layer.getFileN(this.request.getStorageLevel(),filename, this.request.getContentType()));
                }
            response.setContentType(this.request.getContentType());
        }
        
                        //TODO 
                        //FileEntity body = new FileEntity(file, request.getFirstHeader("Accept").toString());
                        //if (request.containsHeader("Accept")){
                        //    Main.log.info("Request Accept : " + request.getFirstHeader("Accept").getValue());
                        //}
                        
                
        return response;            
    }    
}
