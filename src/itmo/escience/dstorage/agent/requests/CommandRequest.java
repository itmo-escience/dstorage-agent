package itmo.escience.dstorage.agent.requests;

import itmo.escience.dstorage.agent.Agent;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.handlers.AgentGetFileHandler;
import itmo.escience.dstorage.agent.handlers.DownloadFileHandler;
import itmo.escience.dstorage.agent.handlers.LvlMgmtHandler;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import itmo.escience.dstorage.agent.utils.AgentCommand;
import itmo.escience.dstorage.agent.utils.AgentCommandParam;
import itmo.escience.dstorage.agent.utils.AgentHttpHeaders;
import itmo.escience.dstorage.agent.utils.AgentMessage;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;

/**
 *
 * @author anton
 */
public class CommandRequest extends AgentRequest {
    
    private AgentResponse response;
    /*
    id (файла)
cmd - copy/move
lvl1 - 0,1,2 (from)
lvl2 - 0,1,2 (to)
    /command/cmd=copy(move)&lvlfrom=0,1,2&lvlto=0,1,2
    /command/cmd=get&uri=192.168.0.10:8080&id=7878
    */
    private final static AgentRequestType type=AgentRequestType.CMD; 
    private AgentCommand commandType;
    private Map<String,String> param=new HashMap();
    
    public CommandRequest(HttpRequest httpRequest){
        super(httpRequest);
        parseRequestLine(httpRequest.getRequestLine().getUri());
        response=new SimpleResponse();
        
    }
    private void parseRequestLine(String line){
        Agent.log.info("Request line parse = " +line);
        String[] parseStr=line.split("[/]");
        for(String s:parseStr[2].split("&")){
            String[] p=s.split("=");
            param.put(p[0].toUpperCase(),p[1]);
        }
        Agent.log.info("Parse result: " +param.toString());
        if(param.containsKey(AgentRequestType.CMD.name()))commandType=AgentCommand.valueOf(param.get(AgentRequestType.CMD.name()).toUpperCase());        
    }
    public boolean isParam(String key){return param.containsKey(key);}
    public String getParam(String key){return param.get(key);}
    @Override
    public AgentRequestType getType() {return type;}
    
    public AgentCommand getAgentCommand(){return commandType;}

    @Override
    public AgentResponse process() {
        switch(getAgentCommand()){
            case LVLMOVE:
            case LVLDELETE:
                if(!isValidRequestParam()){
                    response.setStatus(HttpStatus.SC_BAD_REQUEST);
                    response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("Not enough param in request"+" "+getTarget(), 
                    AgentSystemStatus.FAILED));     
                    Agent.log.info(response.getMessage());
                    return response;
                }
                LvlMgmtHandler lvlHandlerDel=new LvlMgmtHandler(this);
                Thread threadLvlDel=new Thread(lvlHandlerDel);
                threadLvlDel.start();
                break;
            case LVLCOPY:
                if(!isValidRequestParam()){
                    response.setStatus(HttpStatus.SC_BAD_REQUEST);
                    response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("Not enough param in request"+" "+getTarget(), 
                    AgentSystemStatus.FAILED));     
                    Agent.log.info(response.getMessage());
                    return response;
                }
                LvlMgmtHandler lvlHandler=new LvlMgmtHandler(this);
                Thread threadLvl=new Thread(lvlHandler);
                threadLvl.start();
                break;
            case GET:
                if(!isValidRequestParam()){
                    response.setStatus(HttpStatus.SC_BAD_REQUEST);
                    response.setJsonMsg(AgentMessageCreater.createJsonActionResponse("Not enough param in request"+" "+getTarget(), 
                    AgentSystemStatus.FAILED)); 
                    Agent.log.info(response.getMessage());
                    return response;
                }
                AgentGetFileHandler getFileHandler=new AgentGetFileHandler(this);
                Thread threadGetFile=new Thread(getFileHandler);
                threadGetFile.start();
                break;
                //getFileHandler.start();
        }        
        return response;
    }
    private boolean isValidRequestParam(){
        switch(getAgentCommand()){
            case LVLCOPY:
            case LVLMOVE:
                if(!isParam(AgentCommandParam.ID.name()))return false;
                if(!isParam(AgentCommandParam.LVLFROM.name()))return false;
                if(!isParam(AgentCommandParam.LVLTO.name()))return false;
                //if(!isParam(AgentCommandParam.CMD.name()))return false;
                return true;
            case LVLDELETE:
                if(!isParam(AgentCommandParam.ID.name()))return false;
                if(!isParam(AgentCommandParam.LVL.name()))return false;
                return true;
            case GET:
                if(!isParam(AgentCommandParam.ID.name()))return false;
                if(!isParam(AgentCommandParam.URI.name()))return false;
                if(!isParam(AgentCommandParam.LVLFROM.name()))return false;
                if(!isParam(AgentCommandParam.LVLTO.name()))return false;
                return true;
        }
                return false;
    }
    private boolean validateRequest(AgentRequest request){
        if(!(request instanceof CommandRequest)) {
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
    
}
