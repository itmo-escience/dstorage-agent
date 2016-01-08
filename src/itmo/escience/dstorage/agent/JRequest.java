/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.MapReduceMsgProto.MapReduceMsg;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author User
 */
public class JRequest extends ARequest {
    protected JSONObject rawJson;
    
    JRequest(String request){
        parse(request);
    }
    JRequest(){
    }
    
    
    private void parse(String json){ 
         JSONParser jsonParser = new JSONParser();
         Object obj = new Object();
         try {
             obj = jsonParser.parse(json);
         } 
         catch (ParseException ex) {
            Main.log.error("JSON Parse Error. Object:"+ex.getUnexpectedObject()+"; Position:"+ex.getPosition());
         }
         catch (Exception ex) {                  
            Main.log.error("JSON Exception:"+ex.getMessage());
         }          
         this.rawJson=(JSONObject)obj;
         //TODO need exception control
         if (rawJson.containsKey("action")){
                     this.type=AgentRequestType.valueOf(rawJson.get("action").toString().toUpperCase());
                 }
    }
    
}
