package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.Commands;
import itmo.escience.dstorage.agent.utils.HttpConn;
import itmo.escience.dstorage.agent.utils.RemoteResourceType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/**
 *
 * 
 */
public class RRExecuter implements Runnable {
    
    //public enum RRType {SSH, FTP, FTPS, CIFS };
    private static RemoteResourceType rrtype;   
    private JSONObject jsonPutRequest;
    private boolean async;
    public JSONObject jStatus;

    public JSONObject getStatus() {
        return jStatus;
    }
    RRExecuter(RemoteResourceType rrtype,JSONObject jsonPutRequest, boolean async){
        this.rrtype=rrtype;
        this.jsonPutRequest=jsonPutRequest;
        this.async=async;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync() {
        this.async = true;
    }
    private void returnStatus(JSONObject jsonStatus){
        try{
            jsonStatus.put("action", "ack");
            jsonStatus.put("agent_ipaddress", Agent.getAgentAddress());
            jsonStatus.put("agent_port", Agent.getConfig().getProperty("AgentPort"));
            HttpConn httpconn = new HttpConn();
        //Agent.getConfig().getProperty("StorageCoreAddress"),Integer.valueOf(Agent.getConfig().getProperty("StorageCorePort"))
            httpconn.setup(Agent.getConfig().getProperty("StorageCoreAddress"),Agent.getConfig().getProperty("StorageCorePort"));     
            httpconn.setMethod("PUT","/agent");
            httpconn.setEntity(jsonStatus);
            httpconn.connect();
        } catch (UnknownHostException ex) {
            Logger.getLogger(RRExecuter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(RRExecuter.class.getName()).log(Level.SEVERE, null, ex);
        }
            
        
    }
    @Override
    public void run(){
        JSONObject jsonStatus;
        Commands.RemoteResourceCommandType remoteResourceCommandRequest=
                            Commands.RemoteResourceCommandType.valueOf(jsonPutRequest.get("name").toString().toUpperCase());
        try{
        switch(rrtype) {
                case SSH:
                    //list,exec,copyto,copyfrom
                    RemoteResourceSsh ssh = new RemoteResourceSsh();   
        
                    ssh.setConfig(jsonPutRequest.get("user").toString(), jsonPutRequest.get("pwd").toString(),
                    InetAddress.getByName(jsonPutRequest.get("host").toString()));

                    final JSONObject json=ssh.connectResource();
                    if (json.get("status").equals(AgentSystem.STATUS_ERROR)){
                        jsonStatus=json;
                        break;
                        }
                    switch(remoteResourceCommandRequest){
                        case EXEC:
                            jsonStatus=ssh.execCommand(jsonPutRequest.get("cmd").toString());
                            break;
                        case LIST:
                            jsonStatus=ssh.getList(jsonPutRequest.get("path").toString());
                            break;
                        case COPYTO:
                            jsonStatus=ssh.copyResource
                                    (jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    !AgentSystem.isGet);
                            break;
                        case COPYFROM:
                            jsonStatus=ssh.copyResource
                                    (jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    AgentSystem.isGet);
                            break;
                        default:
                            Agent.log.error("Unsupported command name");
                            jsonStatus=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR, "Bad request");    
                    }                                     
                    break;
                case CIFS:
                    //list,copyto,copyfrom
                    RemoteResourceCifs cifs = new RemoteResourceCifs(); 
                    cifs.setAuth(jsonPutRequest.get("user").toString(),
                            jsonPutRequest.get("pwd").toString(), 
                            jsonPutRequest.get("host").toString());
                    switch(remoteResourceCommandRequest){
                        case LIST:
                            jsonStatus=cifs.getList(jsonPutRequest.get("path").toString());
                            break;
                        case COPYTO:
                            jsonStatus=cifs.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    !AgentSystem.isGet);
                            break;
                        case COPYFROM:
                            jsonStatus=cifs.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    AgentSystem.isGet);
                            break;
                        default:
                            Agent.log.error("Unsupported command name");
                            jsonStatus=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR, "Bad request");                              
                    }
                    break;
                case FTP:
                    //list,copyto,copyfrom
                    
                    RemoteResourceFtp ftp = new RemoteResourceFtp(); 
                    ftp.setConfig(jsonPutRequest.get("user").toString(),
                            jsonPutRequest.get("pwd").toString(), 
                            jsonPutRequest.get("host").toString());
                    switch(remoteResourceCommandRequest){
                        case LIST:
                            jsonStatus=ftp.getList(jsonPutRequest.get("path").toString(),false);
                            break;
                        case COPYTO:
                            jsonStatus=ftp.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    !AgentSystem.isGet,!AgentSystem.isSecure);
                            break;
                        case COPYFROM:
                            jsonStatus=ftp.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    AgentSystem.isGet,!AgentSystem.isSecure);
                            break;
                        default:
                            Agent.log.error("Unsupported command name");
                            jsonStatus=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR, "Bad request");                           
                    }
                    break;
                case FTPS:
                                        //list,copyto,copyfrom
                    RemoteResourceFtp ftps = new RemoteResourceFtp(); 
                    ftps.setConfig(jsonPutRequest.get("user").toString(),
                            jsonPutRequest.get("pwd").toString(), 
                            jsonPutRequest.get("host").toString());
                    switch(remoteResourceCommandRequest){
                        case LIST:
                            jsonStatus=ftps.getList(jsonPutRequest.get("path").toString(),true);
                            break;
                        case COPYTO:
                            jsonStatus=ftps.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    !AgentSystem.isGet,AgentSystem.isSecure);
                            break;
                        case COPYFROM:
                            jsonStatus=ftps.copyResource(jsonPutRequest.get("localfile").toString(), jsonPutRequest.get("remotefile").toString(),
                                    AgentSystem.isGet,AgentSystem.isSecure);
                            break;
                        default:
                            Agent.log.error("Unsupported command name");
                            jsonStatus=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR, "Bad request");                          
                    }
                    break;
                default:
                    Agent.log.error("Unsupported command type");
                    jsonStatus=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR, "Bad request");
        }//end switch
        //callback to storage
        //Agent.log.info("jsonSTatus="+jsonStatus.toString());
        
        if (this.isAsync()){
            returnStatus(jsonStatus);            
        }
        else this.jStatus=jsonStatus;
        /*
        jsonStatus.put("action", "ack");
        jsonStatus.put("agent_ipaddress", Agent.getAgentAddress());
        jsonStatus.put("agent_port", Agent.getConfig().getProperty("AgentPort"));
        HttpConn httpconn = new HttpConn();
        //Agent.getConfig().getProperty("StorageCoreAddress"),Integer.valueOf(Agent.getConfig().getProperty("StorageCorePort"))
        httpconn.setup(Agent.getConfig().getProperty("StorageCoreAddress"),Agent.getConfig().getProperty("StorageCorePort"));     
        httpconn.setMethod("PUT","/agent");
        httpconn.setEntity(jsonStatus);
        httpconn.connect();
        //JSONObject jsonstatus = httpconn.getResponse();
        //Agent.log.info("Remote Resource Registration Status :"+jsonstatus.toString());
        * */
        } catch (UnknownHostException ex) {
            Logger.getLogger(RRExecuter.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(RRExecuter.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
}
