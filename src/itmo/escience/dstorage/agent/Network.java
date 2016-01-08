package itmo.escience.dstorage.agent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URL;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import javax.xml.bind.DatatypeConverter;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpClientConnection;
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
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//since 10.11.2015 new fields
/*
total_space -> totalHDD
quota -> qoutaHDD
agent_ipaddress -> ip
agent_port ->port

добавить
quotaSSD
totalSSD
quotaRAM
totalRAM
*/
//TODO move agent parameters to separate class and update them accordinly
//TODO create request through handler
public class Network {
        public static void returnAgentStatus(HttpResponse response)throws Exception {
        // Формирование json ответа агента 
        JSONObject jsonStatus = new JSONObject();
        //log.info(AgentInetAddress.getHostAddress());  
        jsonStatus.put("action", "status");
        jsonStatus.put("ip", Main.getAgentAddress());
        jsonStatus.put("port", Main.getConfig().getProperty("AgentPort"));
        jsonStatus.put("freeHDD", Long.toString(AgentSystem.returnCurrentQuota()));
        if(Main.getSsdQuota()!=null && Main.agentSsdDocRoot!=null ) jsonStatus.put("freeSSD", Long.toString(AgentSystem.returnSsdCurrentQuota()));
        if(Main.getMemQuota()!=null) jsonStatus.put("freeRAM", Long.toString(Main.getStorageLayer().getFreeMemToUse()));
        jsonStatus.put("totalHDD", Main.getAgentDocRoot().getTotalSpace());
        jsonStatus.put("totalSSD", Main.getAgentSSDDocRoot().getTotalSpace());
        jsonStatus.put("totalRAM", StorageLayer.getTotalPhisicalMemory());
        response.setStatusCode(HttpStatus.SC_OK);
        Main.log.info(jsonStatus.toString());                        
        StringEntity strEntRequest = new StringEntity (jsonStatus.toString());
        strEntRequest.setContentType("application/json");     
        response.setEntity(strEntRequest);       
    }       
        public static void registerAgent()throws Exception {
        
        JSONObject jsonRegister = new JSONObject();
        jsonRegister.put("action", "register");
        jsonRegister.put("ip",  Main.getAgentAddress());
        jsonRegister.put("port", Main.getConfig().getProperty("AgentPort"));
        if (Main.getConfig().isProperty("ExternalIP")){
            //validate ip address            
            if (!AgentSystem.isIPv4Address(Main.getConfig().getProperty("ExternalIP"))){
                Main.log.info("ExternalIP option has wrong format");
            }
            else {
                jsonRegister.put("external_ip", Main.getConfig().getProperty("ExternalIP"));
            }
        }
        if (Main.getConfig().isProperty("ExternalPort")){
            //TODO char     
            System.out.println("parseint="+Integer.parseInt(Main.getConfig().getProperty("ExternalPort")));
            if (Integer.parseInt(Main.getConfig().getProperty("ExternalPort"))>=0 && Integer.parseInt(Main.getConfig().getProperty("ExternalPort"))<65536 )
                {
                jsonRegister.put("external_port", Main.getConfig().getProperty("ExternalPort"));
                   // Main.log.info("ExternalPort out of range (0-65535)");
            }
            else {
                Main.log.info("ExternalPort out of range (0-65535)");
                //jsonRegister.put("external_port", Main.getConfig().getProperty("ExternalPort"));
            }
        }
        jsonRegister.put("freeHDD", AgentSystem.returnCurrentQuota());
        if(Main.getSsdQuota()!=null && Main.agentSsdDocRoot!=null ) jsonRegister.put("freeSSD", Long.toString(AgentSystem.returnSsdCurrentQuota()));
        if(Main.getMemQuota()!=null) jsonRegister.put("freeRAM", Long.toString(Main.getStorageLayer().getFreeMemToUse()));
        jsonRegister.put("totalHDD", Main.getAgentDocRoot().getTotalSpace());
        jsonRegister.put("pathHDD", Main.getAgentDocRoot().getAbsolutePath());
        jsonRegister.put("pathSSD", Main.getAgentSSDDocRoot().getAbsolutePath());
        jsonRegister.put("totalSSD", Main.getAgentSSDDocRoot().getTotalSpace());
        jsonRegister.put("totalRAM", StorageLayer.getTotalPhisicalMemory());
        jsonRegister.put("version",Main.getAgentVersion());
        jsonRegister.put("region",Main.getRegion());  
        jsonRegister.put("java_version",System.getProperty("java.version"));    
        jsonRegister.put("os_arch",System.getProperty("os.arch")); 
        jsonRegister.put("os_name",System.getProperty("os.name")); 
        jsonRegister.put("os_version",System.getProperty("os.version")); 
        HttpParams params = new SyncBasicHttpParams();
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpProtocolParams.setUserAgent(params, "DSagent");
        HttpProtocolParams.setUseExpectContinue(params, true);
        
        HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
                // Required protocol interceptors
                new RequestContent(),
                new RequestTargetHost(),
                // Recommended protocol interceptors
                new RequestConnControl(),
                new RequestUserAgent(),
                new RequestExpectContinue()});
        HttpRequestExecutor httpexecutor = new HttpRequestExecutor();
        HttpContext context = new BasicHttpContext(null);

        HttpHost host = new HttpHost(Main.getConfig().getProperty("StorageCoreAddress"),Integer.valueOf(Main.getConfig().getProperty("StorageCorePort")));        
        DefaultHttpClientConnection conn = new DefaultHttpClientConnection();
        context.setAttribute(ExecutionContext.HTTP_CONNECTION, conn);
        context.setAttribute(ExecutionContext.HTTP_TARGET_HOST, host);
        Socket socket = new Socket(host.getHostName(), host.getPort());
        conn.bind(socket, params);
        StringEntity strEntRegister = new StringEntity (jsonRegister.toString());
        Main.log.info("JSON to StorageCore:"+jsonRegister.toString());
        strEntRegister.setContentType("application/json");                             
        HttpPost post =new HttpPost("/agent_register");  
        post.setEntity(strEntRegister);
        Main.log.info(">> Register URI: " + post.getRequestLine().getUri());
        post.setParams(params);
        try {
            httpexecutor.preProcess(post, httpproc, context);
            HttpResponse response = httpexecutor.execute(post, conn, context);
            response.setParams(params);
            httpexecutor.postProcess(response, httpproc, context);
            //log.info("<< Register Response: " + response.getStatusLine());
            //log.info(EntityUtils.toString(response.getEntity()));
            //log.info("==============");
            // Get publicKey from json
            HttpEntity enResponse = response.getEntity();
            InputStream inputstreamEnRequest = enResponse.getContent(); 
            JSONParser jsonParser=new JSONParser();
            JSONObject jsonResponse= new JSONObject();
            jsonResponse = (JSONObject)jsonParser.parse(AgentSystem.getStringFromInputStream(inputstreamEnRequest)); 
            if (!(jsonResponse.containsKey("pKey"))){
                       Main.log.error(jsonResponse.toString());
                    return;
                } 
            String pKey = (String)jsonResponse.get("pKey"); 
            byte[] decode64pKey = DatatypeConverter.parseBase64Binary(pKey);
            KeyFactory fact = KeyFactory.getInstance("RSA");
            X509EncodedKeySpec x509KeySpec = new X509EncodedKeySpec(decode64pKey);
            //publicKey = (PublicKey)fact.generatePublic(x509KeySpec);
            Main.setPublicKey((PublicKey)fact.generatePublic(x509KeySpec));
            //Agent.log.info("publicKey="+Main.getPublicKey().toString());
        } catch (Exception ex) {
            Main.log.error("Exception in registerAgent() "+ex.getLocalizedMessage());
        }       
    }   
        public static void doGetFileFromAgent(
                String fileid,
                URL urlAgent, JSONObject jsonRequest ) throws HttpException, IOException, Exception {
        HttpParams params = new SyncBasicHttpParams();
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpProtocolParams.setUserAgent(params, "DSAgent");
        HttpProtocolParams.setUseExpectContinue(params, true);
        
        HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
                // Required protocol interceptors
                new RequestContent(),
                new RequestTargetHost(),
                // Recommended protocol interceptors
                new RequestConnControl(),
                new RequestUserAgent(),
                new RequestExpectContinue()});
        HttpRequestExecutor httpexecutor = new HttpRequestExecutor();
        HttpContext context = new BasicHttpContext(null);
        //URL urlAgent= new URL (jsonResponse.get("agent_uri").toString());
        Main.log.info("URI host:"+urlAgent.getHost().toString());
        Main.log.info("URI port:"+urlAgent.getPort());
        HttpHost host = new HttpHost(urlAgent.getHost(),urlAgent.getPort());        
        DefaultHttpClientConnection conn = new DefaultHttpClientConnection();
        context.setAttribute(ExecutionContext.HTTP_CONNECTION, conn);
        context.setAttribute(ExecutionContext.HTTP_TARGET_HOST, host);
        if (!conn.isOpen()) {
                    Socket socket = new Socket(host.getHostName(), host.getPort());
                    conn.bind(socket, params);
        }
        BasicHttpRequest requestGet = new BasicHttpRequest("GET", urlAgent.getFile()+fileid);
        //BasicHttpRequest requestGet = new BasicHttpRequest("GET", urlAgent.getFile()+file.getName());
        Main.log.info(">> Request URI: " + requestGet.getRequestLine().getUri());
        requestGet.setParams(params);
        httpexecutor.preProcess(requestGet, httpproc, context);
        HttpResponse responseOnGet = httpexecutor.execute(requestGet, conn, context);
        responseOnGet.setParams(params);
        if ((Main.getConfig().isProperty("Security"))){
            if (Main.getConfig().getProperty("Security").equals("2")){
            requestGet.setHeader("Ticket", jsonRequest.get("Ticket").toString());
            requestGet.setHeader("Sign", jsonRequest.get("Sign").toString());
            }
        }
        httpexecutor.postProcess(responseOnGet, httpproc, context);
        Main.log.info("<< Response: " + responseOnGet.getStatusLine());
          
        if (String.valueOf(responseOnGet.getStatusLine().getStatusCode()).equalsIgnoreCase("200")){
            HttpEntity enRequest = responseOnGet.getEntity();
            InputStream inputstreamEnRequest = enRequest.getContent();
            File file = new File(Main.getConfig().getProperty("AgentdocRoot")+File.separatorChar+fileid); 
            //OutputStream outFile = new FileOutputStream(new File(file.getPath()));
            OutputStream outFile = new FileOutputStream(file.getPath());
            int intBytesRead=0;
            byte[] bytes = new byte[4096];
            while ((intBytesRead=inputstreamEnRequest.read(bytes))!=-1)
                outFile.write(bytes,0,intBytesRead);
            outFile.flush();
            outFile.close();      
            conn.close();
            //Статус выполнения операции возвращается в ядро
            JSONObject jsonRequestCore = new JSONObject();
            jsonRequestCore.put("action", "ack");
            jsonRequestCore.put("file_size", Long.toString(file.length()));
            jsonRequestCore.put("id", file.getName());
            jsonRequestCore.put("ip", Main.getAgentAddress());
            jsonRequestCore.put("port", Main.getConfig().getProperty("AgentPort"));
            //jsonRequest.put("agent_ipaddress", urlAgent.getHost());
            //jsonRequest.put("agent_port", String.valueOf(urlAgent.getPort()));
            returnDownloadFileStatus(jsonRequestCore);
        } else{
            //Статус выполнения операции возвращается в ядро
            JSONObject jsonRequestCore = new JSONObject();
            jsonRequestCore.put("action", "ack");
            jsonRequestCore.put("file_size", "0");
            jsonRequestCore.put("id", fileid);
            jsonRequestCore.put("ip", Main.getAgentAddress());
            jsonRequestCore.put("port", Main.getConfig().getProperty("AgentPort"));
            returnDownloadFileStatus(jsonRequestCore);
        }
        Main.log.info("==============");
    }
        
  public static void returnDownloadFileStatus (JSONObject jsonRequest) throws Exception, HttpException{
        
        HttpParams params = new SyncBasicHttpParams();
        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, "UTF-8");
        HttpProtocolParams.setUserAgent(params, "DSagent");
        HttpProtocolParams.setUseExpectContinue(params, false);            
        HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpRequestInterceptor[] {
                // Required protocol interceptors
                new RequestContent(),
                new RequestTargetHost(),
                // Recommended protocol interceptors
                new RequestConnControl(),
                new RequestUserAgent(),
                new RequestExpectContinue()});
        
        HttpRequestExecutor httpexecutor = new HttpRequestExecutor();

        HttpContext context = new BasicHttpContext(null);
       
        HttpHost host = new HttpHost(Main.getConfig().getProperty("StorageCoreAddress"), 
                    Integer.valueOf(Main.getConfig().getProperty("StorageCorePort")));
        
        DefaultHttpClientConnection conn = new DefaultHttpClientConnection();
        ConnectionReuseStrategy connStrategy = new DefaultConnectionReuseStrategy();

        context.setAttribute(ExecutionContext.HTTP_CONNECTION, conn);
        context.setAttribute(ExecutionContext.HTTP_TARGET_HOST, host);
        try {
                if (!conn.isOpen()) {
                    Socket socket = new Socket(host.getHostName(), host.getPort());
                    conn.bind(socket, params);
                 
                Main.log.info(jsonRequest.toString());                        
                StringEntity strEntRequest = new StringEntity (jsonRequest.toString());
                strEntRequest.setContentType("application/json");
                
                //Post to StorageCore with json
                HttpPost post =new HttpPost("/dsagent");  
                post.setEntity(strEntRequest);
                Main.log.info(">> Request URI: " + post.getRequestLine().getUri());
                post.setParams(params);
                httpexecutor.preProcess(post, httpproc, context);
                //parseResponsefromStorageCore
                HttpResponse response = httpexecutor.execute(post, conn, context);
                response.setParams(params);
                httpexecutor.postProcess(response, httpproc, context);
                Main.log.info("<< Response: " + response.getStatusLine());
                //handle response: parse response json from Core and get agent_uri and file_id 
                ///HttpEntity enResponse = response.getEntity();
                //InputStream inputstreamEnRequest = enResponse.getContent(); 
                //JSONParser jsonParser=new JSONParser();
                //jsonResponse = (JSONObject)jsonParser.parse(getStringFromInputStream(inputstreamEnRequest));          
            }                 
                } finally {
            conn.close();    
            }
        //return jsonResponse;
    }        
}
