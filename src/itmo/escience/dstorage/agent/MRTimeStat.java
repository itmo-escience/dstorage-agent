package itmo.escience.dstorage.agent;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * @author anton
 */
public class MRTimeStat {
    String id;
    long t3; //begin map
    long t4; //end map and send res
    long t5; //start reduce
    long t6; //end reduce send res to leader
    long t7; //start leader reduce
    long t8; //end leader reduce end send to core
    private final static long EMPTY_MARKER=-1;
    public boolean isSent;//sent to core or not
    public enum TimeMarker {t1,t2,t3,t4,t5,t6,t7,t8}
    public void setTimeMarker(long time,TimeMarker num){
        //TimeMarker m=m.valueOf(num);
        //switch (TimeMarker.valueOf(num)){
        switch (num){
                case t3: this.t3=time;break;
                case t4: this.t4=time;break;
                case t5: this.t5=time;break;
                case t6: this.t6=time;break;
                case t7: this.t7=time;break;
                case t8: this.t8=time;break;
                default: Main.log.error("TimeMarker type not Found");            
        }
    }
    public JSONObject toJSON(){
        JSONObject json = new JSONObject();
        JSONObject markers= new JSONObject();
        //put t3 to json
        json.put("id", id);    
        //t3 couldn't be relatively
        //markers=addKeyValue(TimeMarker.t3,0,markers);
        //t4 t4-t3
        markers=((t3==0) ? addKeyValue(TimeMarker.t4,EMPTY_MARKER,markers) : addKeyValue(TimeMarker.t4,t4-t3,markers));
        markers=((t4==0)?addKeyValue(TimeMarker.t5,EMPTY_MARKER,markers):addKeyValue(TimeMarker.t5,t5-t4,markers));
        markers=((t5==0)?addKeyValue(TimeMarker.t6,EMPTY_MARKER,markers):addKeyValue(TimeMarker.t6,t6-t5,markers));
        markers=((t6==0)?addKeyValue(TimeMarker.t7,EMPTY_MARKER,markers):addKeyValue(TimeMarker.t7,t7-t6,markers));
        markers=((t7==0)?addKeyValue(TimeMarker.t8,EMPTY_MARKER,markers):addKeyValue(TimeMarker.t8,t8-t7,markers));
        /*
        if (this.t3!=0)
            markers.put("t3", this.t3);        
        else
            markers.put("t3","-1");        
        */
        json.put("action", "ack");
        json.put("agent_ipaddress", Main.getAgentAddress());
        json.put("agent_port", Main.getConfig().getProperty("AgentPort"));  
        //Agent.log.info("markers="+markers.toString());
        json.put("stat", markers);
        return json;
    }
    public void clearMRStat(){
        this.t3=0;
        this.t4=0;
        this.t5=0;
        this.t6=0;
        this.t7=0;
        this.t8=0;
        this.id="";
    }
    private static JSONObject addKeyValue(TimeMarker tm, long marker, JSONObject jo){
        if (marker!=0 && marker>0)
            jo.put(TimeMarker.valueOf(tm.toString()), marker);
        else
            jo.put(TimeMarker.valueOf(tm.toString()), -1);
        return jo;
    }
}
