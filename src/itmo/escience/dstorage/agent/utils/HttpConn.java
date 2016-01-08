package itmo.escience.dstorage.agent.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpClientConnection;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HttpConn {
    private HttpParams params;
    private HttpRequestExecutor httpexecutor;
    //private HttpEntityEnclosingRequestBase httpconnent;
    private BasicHttpEntityEnclosingRequest httpconnent;
    //private HttpRequestBase httpconn; 
    private BasicHttpRequest httpconn; 
    private int statusCode;
    private DefaultHttpClientConnection conn;
    private HttpContext context;
    private HttpProcessor httpproc;
    private HttpEntity responseEntity;
    private String host;
    private int port;
    private boolean isEntity=false;
                
    public void setup(String httpHost, String httpPort) throws Exception{
        params = new SyncBasicHttpParams();
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpProtocolParams.setUserAgent(params, "DSagent");
        HttpProtocolParams.setUseExpectContinue(params, true);        
        httpproc = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
                new RequestContent(),
                new RequestTargetHost(),
                new RequestConnControl(),
                new RequestUserAgent(),
                new RequestExpectContinue()});
        httpexecutor = new HttpRequestExecutor();
        context = new BasicHttpContext(null);
        //HttpHost host = new HttpHost(Agent.getConfig().getProperty("StorageCoreAddress"),Integer.valueOf(Agent.getConfig().getProperty("StorageCorePort"))); 
        HttpHost host = new HttpHost(httpHost,Integer.valueOf(httpPort)); 
        conn = new DefaultHttpClientConnection();
        context.setAttribute(ExecutionContext.HTTP_CONNECTION, conn);
        context.setAttribute(ExecutionContext.HTTP_TARGET_HOST, host);
        Socket socket = new Socket(host.getHostName(), host.getPort());
        conn.bind(socket, params);
    }
    /*
        public void setHost(String host, String port){
        this.host=host;
        this.port=Integer.valueOf(port);        
    }
    public void setHost(){
        this.host = Client.getConfig().getProperty("StorageCoreAddress");
        this.port=Integer.valueOf(Client.getConfig().getProperty("StorageCorePort"));
    }
    */
    public void setEntity( JSONObject json) throws UnsupportedEncodingException{
        StringEntity strEntRegister = new StringEntity (json.toString());
        //Client.log.info("JSON to StorageCore:"+json.toString());
        //Agent.log.info("set entity json:"+json.toString());
        strEntRegister.setContentType("application/json");   
        httpconnent.setEntity(strEntRegister);                
    }
    public void setEntity( InputStreamEntity entity) throws UnsupportedEncodingException{                           
        entity.setContentType("binary/octet-stream");
        entity.setChunked(true);    
        httpconnent.setEntity(entity);
    }
    public void setEntity( ByteArrayEntity entity) throws UnsupportedEncodingException{                           
        entity.setContentType("binary/octet-stream");
        entity.setChunked(true);    
        httpconnent.setEntity(entity);
    }
    
    public void setMethod(String method, String requestline){
        if (method.equals("PUT")){
            //httpconnent =new HttpPost(requestline);            
            httpconnent=new BasicHttpEntityEnclosingRequest ("POST",requestline);
            httpconnent.setParams(params);
            isEntity=true;
        }
        if (method.equals("DELETE")){
            httpconn=new BasicHttpRequest("DELETE",requestline);
            //httpconn=new HttpDelete(requestline); 
            httpconn.setParams(params);
        }
        if (method.equals("GET")){
            
            httpconn=new BasicHttpRequest("GET",requestline); 
            //httpconn=new HttpGet(requestline); 
            httpconn.setParams(params);
        }
    }
    public void setHeader(String nameHeader, String headerContent){
        //debug purpose
        //Agent.log.info("headers in httpconn:"+nameHeader+":"+headerContent);
        httpconn.setHeader(nameHeader,headerContent);
    }
    public void connect() throws HttpException,IOException, CloneNotSupportedException {
        final BasicHttpRequest http;
        if (isEntity) http=httpconnent;
        //else http=(HttpEntityEnclosingRequestBase)httpconn;
        else http= httpconn;
        
        
        httpexecutor.preProcess(http, httpproc, context);
        //TODO coundn't get response
        HttpResponse response = httpexecutor.execute(http, conn, context);
        response.setParams(params);
        
        httpexecutor.postProcess(response, httpproc, context);
        responseEntity= response.getEntity();   
        statusCode=response.getStatusLine().getStatusCode();
        //response.getStatusLine();

    }
    public int getStatusCode(){return statusCode;}
    public JSONObject getResponse() throws IOException{
        String entityContent = EntityUtils.toString(responseEntity);
        return parseJSON(entityContent);
    }
    public void close() throws IOException{
        conn.close();
    }
    public InputStream getInputStreamResponse() throws IOException{
        return responseEntity.getContent();
    } 
        public static JSONObject parseJSON(String obj) {
             
         JSONParser jsonParser = new JSONParser();
         JSONObject jsonObject = new JSONObject();
         try {
             jsonObject = (JSONObject)jsonParser.parse(obj);
         } 
         catch (ParseException ex) {
            System.out.println("JSON Parse Error. Object:"+ex.getUnexpectedObject()+"; Position:"+ex.getPosition());
            return jsonObject; 
         }
         catch (Exception ex) {                  
            System.out.println("JSON Exception:"+ex.getMessage());
            return jsonObject; 
         } 
         
         return jsonObject;
    }
}