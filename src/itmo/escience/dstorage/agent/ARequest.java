package itmo.escience.dstorage.agent;

import java.util.Map;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ARequest {
    protected AgentRequestType type;
    protected JSONObject rawJson;
    public enum AgentRequestType {MAP, REDUCE, LEADER, REMOTERESOURCE, ZIP, MONITOR };
    public HttpHeaders hheaders;
    public String requestLine;
    public HttpRequest request;
    public HttpResponse response;
    //AgentRequest(JSONObject json){
    //    parse(json);
    //}
    //ARequest(String request ){
    //    parse(request);
    //}
    public void setHeaders(HttpRequest request){
        hheaders=new HttpHeaders();
        hheaders.initHeaders(request);
        
    }
    public Map<String,String> getHeaders(){
        return hheaders.getHeaders();
    }
    public void init(HttpRequest request, HttpResponse response){
        this.setHeaders(request);
        this.request=request;
        this.response=response;
        this.requestLine=request.getRequestLine().getUri();
    }
    public void process() throws Exception{
        
    }
    //private void parse(JSONObject json){      
    //
    //}
    /*
    private void parse(String json){ 
         JSONParser jsonParser = new JSONParser();
         Object obj = new Object();
         try {
             obj = jsonParser.parse(json);
         } 
         catch (ParseException ex) {
            Agent.log.error("JSON Parse Error. Object:"+ex.getUnexpectedObject()+"; Position:"+ex.getPosition());
         }
         catch (Exception ex) {                  
            Agent.log.error("JSON Exception:"+ex.getMessage());
         }          
         this.rawJson=(JSONObject)obj;
         //TODO need exception control
         if (rawJson.containsKey("action")){
                     this.type=AgentRequestType.valueOf(rawJson.get("action").toString().toUpperCase());
                 }
    }
    * */
    
}
