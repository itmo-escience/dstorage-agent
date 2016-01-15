package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.Network;
import itmo.escience.dstorage.agent.StorageLayer;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.CommandRequest;
import itmo.escience.dstorage.agent.requests.DownloadFileRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.CoreAckResponse;
import itmo.escience.dstorage.agent.responses.DownloadFileResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import itmo.escience.dstorage.agent.utils.AgentCommand;
import itmo.escience.dstorage.agent.utils.AgentCommandParam;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.HttpConn;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;

/**
 *
 * @author anton
 */
public class LvlMgmtHandler implements Runnable {
    private CommandRequest request;
    private CoreAckResponse coreResponse;
    
    public LvlMgmtHandler(AgentRequest agentRequest){        
        this.request=(CommandRequest)agentRequest;
        coreResponse=new CoreAckResponse();
    }
    private long lvlCopy(String filename,StorageLevel from,StorageLevel to){
        StorageLayer layer=Main.getStorageLayer(); 
        InputStream is=layer.getFileAsStream(from, filename);
        if(is==null )return 0;
        return layer.addFile(to, is, filename);
    }
    private long lvlDelete(String filename,StorageLevel from){
        StorageLayer layer=Main.getStorageLayer(); 
        long size=layer.getFileLen(from, filename);
        return layer.deleteFileFromLevel(from,filename)? 0 : size;
    }
    private long lvlMove(String filename,StorageLevel from,StorageLevel to){
        StorageLayer layer=Main.getStorageLayer(); 
        InputStream is=layer.getFileAsStream(from, filename);
        if(is==null )return 0;
        //copy
        long size=0;
        size=layer.addFile(to, is, filename);
        
        //delete
        if(size>0) {
            if(!layer.deleteFileFromLevel(from,filename)) return 0;
            else return size;
        }     
        else return 0;
    }

    @Override
    public void run() {
        StorageLayer layer=Main.getStorageLayer();                       
        String filename=request.getParam(AgentCommandParam.ID.name());
        //check that file is located on apropriate level        
        long status=0;
        /*if(!layer.isFileOnLevel(levelFrom,filename )){            
            coreResponse.setJsonMsg(AgentMessageCreater.createJsonActionResponse("File not found on level"+" "+request.getTarget(), 
                    AgentSystemStatus.FAILED));     
        }*/ 
        //else{        
            switch(request.getAgentCommand()){
                case COPY:
                    if(!request.getParam(AgentCommandParam.URI.name()).equals(Main.getAgentAddress()+":"+Main.getAgentPort()))//it is remote agent
                    {
                        status=downloadFile();
                    }
                    else //local file
                    status=lvlCopy(filename,StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLFROM.name()))),
                            StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLTO.name()))));
                    break;
                case MOVE:
                    status=lvlMove(filename,StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLFROM.name()))),
                            StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLTO.name()))));
                    break;
                case DELETE:
                    status=lvlDelete(filename,StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVL.name()))));
                    break;
            }
        //}
        //TODO Sender class
        coreResponse.setFileId(filename);
        coreResponse.setSize(status);
        if(request.getAgentCommand().equals(AgentCommand.DELETE))
            coreResponse.setLvl(StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVL.name()))));
        else 
            coreResponse.setLvl(StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLTO.name()))));
        coreResponse.setCmd(request.getAgentCommand());
        if(request.isParam(AgentCommandParam.CMDID.name()))
            coreResponse.setCmdID(Long.parseLong(request.getParam(AgentCommandParam.CMDID.name())));
        try {
            Network.returnDownloadFileStatus(coreResponse.getJSON());
        } catch (Exception ex) {
            Logger.getLogger(LvlMgmtHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private long downloadFile(){
        long status=0L;
        try {
            String uri=request.getParam(AgentCommandParam.URI.name());
            String fileid=request.getParam(AgentCommandParam.ID.name());
            StorageLevel levelfrom=StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLFROM.name())));
            StorageLevel levelto=StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLTO.name())));
            String ip=uri.split(":")[0];
            String port=uri.split(":")[1];
            HttpConn httpconn = new HttpConn();
            httpconn.setup(ip,port);
            httpconn.setMethod("GET",fileid);
            if ((Main.getConfig().isProperty("Security"))){
                if (Main.getConfig().getProperty("Security").equals("2")){                    
                    httpconn.setHeader("Ticket", request.getHeader("Ticket"));
                    httpconn.setHeader("Sign", request.getHeader("Sign")); 
                    httpconn.setHeader("StorageLevel",String.valueOf(levelfrom.getNum()));
                }
            }
            httpconn.connect();
            InputStream is = httpconn.getInputStreamResponse();
            status=Main.getStorageLayer().addFile(levelto, is, fileid);             
            httpconn.close();
        } catch (Exception ex) {
            Logger.getLogger(AgentGetFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return status;
    }
}
