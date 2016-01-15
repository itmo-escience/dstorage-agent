package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.AgentSystem;
import itmo.escience.dstorage.agent.Network;
import itmo.escience.dstorage.agent.StorageLayer;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.UploadFileRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import itmo.escience.dstorage.agent.utils.AgentHttpHeaders;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpStatus;
import org.json.simple.JSONObject;

/**
 *
 * @author Anton Spivak
 */
public class UploadFileHandler implements IRequestHandler{
    private UploadFileRequest request;
    private SimpleResponse response=new SimpleResponse();
    
    private boolean validateRequest(){
        if(!(request instanceof UploadFileRequest)) {
            response.setStatus(HttpStatus.SC_BAD_REQUEST);
            response.setMessage(AgentMessage.FAILED);
            return false;
        }
        if(!Ticket.validateStorageTicket(request)){
            response.setStatus(HttpStatus.SC_FORBIDDEN);
            response.setMessage(AgentMessage.FORBIDDEN);
            return false;
        }
        return true;
    }    
    @Override
    public AgentResponse handle(AgentRequest request) {
        this.request=(UploadFileRequest)request;
        if(!validateRequest())return response;        
        StorageLayer layer=Main.getStorageLayer();
        String filename = this.request.getTarget();
        //delete slash
        if(filename.startsWith("/"))filename=filename.substring(1);
        if (layer.isFileExist(filename)) {
            response.setStatus(HttpStatus.SC_FORBIDDEN);
            response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("File "+filename +" already exist", 
                    AgentSystemStatus.FAILED));     
            return response;
        } 
        //check that options of ssd and mem indicated in config if else return error response
        if(!this.request.getStorageType().equals(StorageLevel.HDD)){
            if(this.request.getStorageType().equals(StorageLevel.SSD) && (Main.getSsdQuota()==null || Main.agentSsdDocRoot==null)){
                response.setStatus(HttpStatus.SC_CONFLICT);
                response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("Needed for processing the request options (SSDQuota or AgentSSDdocRoot) "
                        + "not set in config", 
                    AgentSystemStatus.FAILED));     
                return response;
            }
            if(this.request.getStorageType().equals(StorageLevel.MEM) && Main.getMemQuota()==null){
                response.setStatus(HttpStatus.SC_CONFLICT);
                response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("Needed for processing the request options (RAMQuota) not set in config", 
                    AgentSystemStatus.FAILED));     
                return response;
            }            
        }        
        long filesize=0;
        try {
            filesize=layer.addFile(this.request.getStorageType(), this.request.getEntity().getContent(), filename);
        } catch (IOException ex) {
                Logger.getLogger(UploadFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalStateException ex) {
                Logger.getLogger(UploadFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }        
        /*
        InputStream inputstreamEnRequest;
        OutputStream outFile;
        try {
            inputstreamEnRequest = enRequest.getContent();
            outFile = new FileOutputStream(Main.getAgentDocRoot().getPath() +File.separatorChar+ file.getName());             
        int intBytesRead=0;
        byte[] bytes = new byte[4096];
        while ((intBytesRead=inputstreamEnRequest.read(bytes))!=-1)
            outFile.write(bytes,0,intBytesRead);
            outFile.flush();
        */
            //Статус выполнения операции возвращается в ядро
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("action", "ack");
            jsonRequest.put("file_size", Long.toString(filesize));
            jsonRequest.put("id", filename);
            if(request.checkHeaders(new String[]{AgentHttpHeaders.CmdID.getHeaderString()})) 
                jsonRequest.put("cmdid", (Long)((UploadFileRequest)request).getCmdid());
            //if(request.checkHeaders(new String[]{AgentHttpHeaders.StorageLevel.getHeaderString()})) 
            //jsonRequest.put("lvl", ((UploadFileRequest)request).getStorageType().getNum());
            //jsonRequest.put("lvlto", layer.getFileStorageType(filename).getNum());
            jsonRequest.put("ip", Main.getAgentAddress());
            jsonRequest.put("port", Main.getConfig().getProperty("AgentPort"));                  
            try {    
                Network.returnDownloadFileStatus(jsonRequest);
            } catch (Exception ex) {
                Main.log.error("Exception in handle "+ex.getMessage());
            }                                        
        return this.response;
    }
}
