package itmo.escience.dstorage.agent.handlers;

import itmo.escience.dstorage.agent.Agent;
import itmo.escience.dstorage.agent.utils.HttpConn;
import itmo.escience.dstorage.agent.Network;
import itmo.escience.dstorage.agent.StorageLayer;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.requests.CommandRequest;
import itmo.escience.dstorage.agent.responses.CoreAckResponse;
import itmo.escience.dstorage.agent.utils.AgentCommandParam;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpStatus;

/**
 *
 * @author anton
 * 
 * Обработчик для получения файлов агентом с других агентов. Обрабатывает CommandRequest запрос,в котором команда выполнения 
 * 
 */
public class AgentGetFileHandler implements Runnable{
    private CommandRequest request;
    private CoreAckResponse coreResponse;
    
    public AgentGetFileHandler(CommandRequest agentRequest){
        this.request=(CommandRequest)agentRequest;
        coreResponse=new CoreAckResponse();
    }   
    @Override
    public void run() {
        downloadFile();    
        
        //TODO Sender class
        try {
            Network.returnDownloadFileStatus(coreResponse.getJSON());
        } catch (Exception ex) {
            Logger.getLogger(LvlMgmtHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private void downloadFile(){
        try {
            String uri=request.getParam(AgentCommandParam.URI.name());
            String fileid=request.getParam(AgentCommandParam.ID.name());
            
            String ss=request.getParam(AgentCommandParam.LVLFROM.name());
            int r=Integer.parseInt(request.getParam(AgentCommandParam.LVLFROM.name()));
            StorageLevel levelfrom=StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLFROM.name())));
            StorageLevel levelto=StorageLevel.getLevelById(Integer.parseInt(request.getParam(AgentCommandParam.LVLTO.name())));
            String ip=uri.split(":")[0];
            String port=uri.split(":")[1];
            HttpConn httpconn = new HttpConn();
            httpconn.setup(ip,port);
            httpconn.setMethod("GET",fileid);
            if ((Agent.getConfig().isProperty("Security"))){
                if (Agent.getConfig().getProperty("Security").equals("2")){
                    if (!Ticket.validateStorageTicket(request)) return;
                    httpconn.setHeader("Ticket", request.getHeader("Ticket"));
                    httpconn.setHeader("Sign", request.getHeader("Sign")); 
                    httpconn.setHeader("StorageLevel",String.valueOf(levelfrom.getNum()));
                }
            }
            httpconn.connect();
            InputStream is = httpconn.getInputStreamResponse();
            if(request.isParam(AgentCommandParam.CMDID.name()))
                coreResponse.setCmdID(Long.parseLong(request.getParam(AgentCommandParam.CMDID.name())));
            coreResponse.setFileId(fileid);
            coreResponse.setLvl(levelto);
            if(httpconn.getStatusCode()==HttpStatus.SC_OK)
                coreResponse.setSize(Agent.getStorageLayer().addFile(levelto, is, fileid));    
            else
                coreResponse.setSize(0L);
            coreResponse.setCmd(request.getAgentCommand());
            httpconn.close();
        } catch (Exception ex) {
            Logger.getLogger(AgentGetFileHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }    
}
