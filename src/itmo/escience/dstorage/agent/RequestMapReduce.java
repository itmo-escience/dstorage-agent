package itmo.escience.dstorage.agent;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import itmo.escience.dstorage.agent.MapReduceMsgProto.MapReduceMsg;
import org.json.simple.JSONValue;

public class RequestMapReduce extends JRequest {
    protected MapReduceRequestType type;  
    protected Map<String,String> files;
    protected String name;
    protected String options="";
    protected String leaderURI;
    protected String backURI;
    protected JSONArray agentList;
    protected int agentCount;
    protected byte[] data;
    protected String sessionID;
    protected String task_id;
    protected int count;
    protected String proto;
    protected String datafile;
    //protected Object[] arrayOb;
    public enum MapReduceRequestType {MAP, REDUCE, LEADER };
    //RequestMapReduce(){
    //}
    RequestMapReduce(String json) throws Exception{
        super(json);
        agentList=new JSONArray();
        //TODO need exception control
        this.name=rawJson.get("name").toString();
        this.task_id=rawJson.get("id").toString();
        if (rawJson.containsKey("uri")){
            this.leaderURI=rawJson.get("uri").toString();
            if (leaderURI.contains("//"))leaderURI=leaderURI.split("//")[1];
        }
        
        if (rawJson.containsKey("options"))
            this.options=rawJson.get("options").toString();
        this.backURI=rawJson.get("back_uri").toString();
        if (backURI.contains("//"))backURI=backURI.split("//")[1];
        //TODO check
        this.type=MapReduceRequestType.valueOf(super.type.toString());
        
        //TODO exception control
        if(rawJson.containsKey("list")){
            JSONObject list =(JSONObject)rawJson.get("list");
        files=new HashMap<String,String>();        
        Iterator iter = list.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next(); 
            files.put(entry.getKey().toString(), entry.getValue().toString());
        }
        }
        agentList =(JSONArray)rawJson.get("agent_list");
        this.agentCount=agentList.size();
        if(rawJson.containsKey("sessionID")){
            this.sessionID=rawJson.get("sessionID").toString();
        }
        if(rawJson.containsKey("data")){
            //check that null
            if (rawJson.get("data")==null) this.datafile=null;
            else    this.datafile=rawJson.get("data").toString();
        }
        if(rawJson.containsKey("count")){
            this.count=Integer.parseInt(rawJson.get("count").toString());
        }
        if(rawJson.containsKey("proto")){
            if (rawJson.get("proto")==null) this.proto=null;
            else this.proto=rawJson.get("proto").toString();
        }
            if (rawJson.containsKey("list"))
                this.count=files.size();                        
    }
   
    RequestMapReduce(MapReduceMsg msg){
        super();
        this.type=MapReduceRequestType.valueOf(msg.getAction().toString());
        this.data=msg.getData().toByteArray();
        this.backURI=msg.getBackUri();
        this.leaderURI=msg.getLeaderUri();
        this.options=msg.getOptions();
        this.count=msg.getCount();
        this.sessionID=msg.getSessionId();
        this.task_id=msg.getId();
        this.name=msg.getName();     
        Object obj=JSONValue.parse(msg.getAgentList());
        this.agentList=(JSONArray)obj;
        this.agentCount=this.agentList.size();
        this.proto=msg.getProto();
        this.datafile=msg.getDatafile();
    }

    public int getAgentCount(){
        return agentList.size();
    }
    public JSONObject requestConstPart(){
        JSONObject json=new JSONObject();
        json.put("name",name);
        json.put("uri",leaderURI);
        json.put("back_uri",backURI);
        json.put("agent_list",agentList);
        json.put("id",task_id);
        json.put("proto", proto);
        json.put("datafile", datafile);
        return json;
                
    }  
}