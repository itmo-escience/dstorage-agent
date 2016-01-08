package itmo.escience.dstorage.agent;

import java.util.HashMap;
import java.util.Map;
import org.apache.http.Header;
import org.apache.http.HttpRequest;

public class HttpHeaders {
    //Collections of http request headers
    public Map<String,String> headers;
    
    public void initHeaders(HttpRequest request){
        Map<String,String> headers=new HashMap<String,String>();
        Header[] allHeaders=request.getAllHeaders();
        for (int i=0;i<allHeaders.length;i++){
            //Agent.log.info("set headers :"+allHeaders[i].getName()+allHeaders[i].getValue());
            headers.put(allHeaders[i].getName(), allHeaders[i].getValue());
        }
            //   for (Map.Entry<String, String> entry: this.headers.entrySet())
            Agent.log.info("Headers ticket:"+headers.get("Ticket"));
            this.headers=headers;
        /*
        if (request.containsHeader("Ticket") && request.containsHeader("Sign") ){
            strTicket=request.getFirstHeader("Ticket").getValue();
            strSign=request.getFirstHeader("Sign").getValue();     
        }
        */             
    }
    public boolean isTicket(){
        if (headers.keySet().contains("Ticket") && headers.keySet().contains("Sign")) return true;
        return false;
    }
    public Map<String,String> getHeaders(){
        return headers;
    }
}
