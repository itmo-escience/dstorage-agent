package itmo.escience.dstorage.agent.responses;

import itmo.escience.dstorage.agent.Main;
import itmo.escience.dstorage.agent.AgentSystem;
import itmo.escience.dstorage.agent.utils.AgentCommand;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import org.json.simple.JSONObject;

/**
 *
 * @author anton
 */
public class CoreAckResponse {
    private String jsonMsg="";
    private String filename;
    private long size;
    private AgentCommand cmd;
    private long cmdid=-1;
    private StorageLevel lvl;
    
    public void setFileId(String id){filename=id;}
    public void setSize(long s){size=s;}
    public void setCmd(AgentCommand c){cmd=c;}
    public void setLvl(StorageLevel l){lvl=l;}
    public void setCmdID(long id){cmdid=id;}
    public void setJsonMsg(String s){this.jsonMsg=s;} 
    public JSONObject getJSON(){
        if(!jsonMsg.equals(""))
            return AgentSystem.parseJSON(jsonMsg);
        JSONObject json = new JSONObject();
        json.put("action", "ack");
        json.put("file_size", Long.toString(size));
        json.put("id", filename);
        json.put("ip", Main.getAgentAddress());
        if(cmd!=null)
            json.put("cmd", cmd.name());
        if(cmdid!=-1)
            json.put("cmdid", cmdid);
        if(lvl!=null)
            json.put("lvl", lvl.getNum());
        json.put("port", Main.getConfig().getProperty("AgentPort"));
        return json;
    }
}
