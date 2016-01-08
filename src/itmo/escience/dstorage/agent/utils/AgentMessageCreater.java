package itmo.escience.dstorage.agent.utils;

import itmo.escience.dstorage.agent.AgentSystem;
import org.json.simple.JSONObject;

/**
 *
 * @author anton
 */
public class AgentMessageCreater {
    
    public static String createJsonActionResponse(String msg,AgentSystemStatus status){
        JSONObject json = new JSONObject();
        json.put("action", "response");
        json.put("status", status.getString());
        json.put("msg", msg);
        return json.toString();
    }
    public static String createJsonActionAck(String msg,AgentSystemStatus status){
        JSONObject json = new JSONObject();
        json.put("action", "ack");
        json.put("status", status.getString());
        json.put("msg", msg);
        return json.toString();
    }
    public static String createJsonError(String msg,AgentSystemStatus status){
        JSONObject json = new JSONObject();
        json.put("status", status.getString());
        json.put("msg", msg);
        return json.toString();
    }
}
