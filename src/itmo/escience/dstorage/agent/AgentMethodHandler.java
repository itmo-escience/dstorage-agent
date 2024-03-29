package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.handlers.UploadFileHandler;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.DownloadFileRequest;
import itmo.escience.dstorage.agent.requests.RequestFactory;
import itmo.escience.dstorage.agent.requests.UploadFileRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.DownloadFileResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.util.Date;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;

public class AgentMethodHandler {
    
    
        public static void handleMethodDELETE(HttpRequest request, HttpResponse response)throws Exception { 

        String target = request.getRequestLine().getUri();
        //this.docroot
        final File file = new File(Main.getAgentDocRoot().getPath(), URLDecoder.decode(target,Main.getLocalEncoding()));
        String str=URLDecoder.decode(target,Main.getLocalEncoding());
        JSONObject jsonRequest = new JSONObject();    
            Main.log.info("URLDecoder = "+URLDecoder.decode(target,Main.getLocalEncoding()));    
        if ((Main.getConfig().isProperty("Security"))){
            if (Main.getConfig().getProperty("Security").equals("2"))
                {
                if (!Ticket.validateStorageTicket(request)) {
                    response.setStatusCode(HttpStatus.SC_FORBIDDEN);
                    return;
                    }
                }
        }
                    boolean boolRes=file.delete();
                    //Статус выполнения операции возвращается в ядро
                    //JSONObject jsonRequest = new JSONObject();
                    jsonRequest.put("action", "ack");
                    //jsonRequest.put("file_size", Long.toString(file.length()));
                    jsonRequest.put("id", file.getName());
                    jsonRequest.put("agent_ipaddress", Main.getAgentAddress());
                    jsonRequest.put("agent_port", Main.getConfig().getProperty("AgentPort"));                  
                    if (!boolRes)
                        Main.log.error("Failed to delete file:" + file.getPath());
                    else
                        Main.log.info("Deleted file:" + file.getPath());
                    try {    
                    Network.returnDownloadFileStatus(jsonRequest);
                    } catch (Exception ex) {
                        Main.log.error("Exception in handle"+ex.toString());
                    }                                       
    }            
    public static void handleMethodPUT(HttpRequest request, HttpResponse response)throws Exception {
        
        Main.log.info("URL in PUT "+URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        //Call RemoteResource handler command
        if ((request instanceof HttpEntityEnclosingRequest) && 
                URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/remoteresource")) {
                        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();
                        String entityContent = EntityUtils.toString(enRequest);
                        Main.log.info("Incoming PUT Request:\n" + entityContent);
                        JSONObject jsonPutRequest = AgentSystem.parseJSON(entityContent);
                        Main.log.info("JSON incoming PUT Request on /remoteresource = " + jsonPutRequest);
                        JSONObject jsonStatus=new JSONObject();
                        //TO DO if not action generation error
                        if (jsonPutRequest.containsKey("action")){
                            jsonStatus=Commands.handleRRCommand(jsonPutRequest);
                            response.setStatusCode(HttpStatus.SC_OK);
                        } else {
                            jsonStatus.put("msg", "Bad request");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                        }    
                        Main.log.info(jsonStatus.toString()); 
                        StringEntity strEntRequest = new StringEntity (jsonStatus.toString(),System.getProperty("file.encoding"));
                        strEntRequest.setContentType("application/json");     
                        response.setEntity(strEntRequest);
                        return;     
                }
        //Call zip handler command
        //TODO Refactoring with switch by getUri
        if ((request instanceof HttpEntityEnclosingRequest) && 
                URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/zip")) {
                        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();
                        String entityContent = EntityUtils.toString(enRequest);
                        Main.log.info("Incoming PUT Request:\n" + entityContent);
                        JSONObject jsonPutRequest = AgentSystem.parseJSON(entityContent);
                        Main.log.info("JSON incoming PUT Request on /zip = " + jsonPutRequest);
                        JSONObject jsonStatus=null;
                        //TO DO if not action generate error
                        if (jsonPutRequest.containsKey("action")){
                            jsonStatus=Commands.zip(jsonPutRequest);
                            response.setStatusCode(HttpStatus.SC_OK);
                        } else {
                            jsonStatus.put("msg", "Bad request");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                        }
                        response.setStatusCode(HttpStatus.SC_OK);      
                        Main.log.info(jsonStatus.toString()); 
                        StringEntity strEntRequest = new StringEntity (jsonStatus.toString(),System.getProperty("file.encoding"));
                        strEntRequest.setContentType("application/json");     
                        response.setEntity(strEntRequest);
                        return;     
                }
        //Only map from core command
                if ((request instanceof HttpEntityEnclosingRequest) && 
                URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/map")){                                              
                        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();
                        String entityContent = EntityUtils.toString(enRequest);
                        Main.log.info("Incoming PUT Request:\n" + entityContent);
                        JSONObject jsonPutRequest = AgentSystem.parseJSON(entityContent);
                        Main.log.info("JSON incoming PUT Request on /map_reduce = " + jsonPutRequest);
                        JSONObject jsonStatus=null;
                        //write t1 marker
                        //need refactor
                        if (jsonPutRequest.get("action").equals("map"))
                        Main.getMapReduceStat().addMrTimeStat(jsonPutRequest.get("id").toString(), new Date().getTime(), MRTimeStat.TimeMarker.t3);
                        /* not needed any more
                        if (jsonPutRequest.get("action").equals("reduce"))
                        Main.getMapReduceStat().addMrTimeStat(jsonPutRequest.get("id").toString(), new Date().getTime(), MRTimeStat.TimeMarker.t5);
                        if (jsonPutRequest.get("action").equals("leader"))
                        Main.getMapReduceStat().addMrTimeStat(jsonPutRequest.get("id").toString(), new Date().getTime(), MRTimeStat.TimeMarker.t7);
                        */
                        //RequestMapReduce request  = new RequestMapReduce(jsonMapReduce.toString());
                        //TO DO if not action generate error
                        if (jsonPutRequest.containsKey("action")){
                            jsonStatus=Commands.mapReduce(new RequestMapReduce(jsonPutRequest.toString()));
                            Main.log.info("After call back from Command.mapreduce:"+jsonStatus.toString());
                            response.setStatusCode(HttpStatus.SC_OK);
                        } else {
                            jsonStatus.put("msg", "Bad request");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                        }
                        response.setStatusCode(HttpStatus.SC_OK);  
                        if(jsonStatus==null || jsonStatus.isEmpty()){
                            jsonStatus=new JSONObject();
                            jsonStatus.put("msg", "Error");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR); 
                        }
                        Main.log.info(jsonStatus.toString()); 
                        StringEntity strEntRequest = new StringEntity (jsonStatus.toString(),System.getProperty("file.encoding"));
                        strEntRequest.setContentType("application/json");     
                        response.setEntity(strEntRequest);
                        return;     
                }
         //Reduce and leader action of MapReduce function (after implement protobuf)
                if ((request instanceof HttpEntityEnclosingRequest) && 
                URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/reduce")){                      
                        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();
                        MapReduceMsgProto.MapReduceMsg msgread=null;        
                        msgread = MapReduceMsgProto.MapReduceMsg.parseFrom(enRequest.getContent());
                        Main.log.info("Incoming PUT Request:");
                        //JSONObject jsonPutRequest = AgentSystem.parseJSON(entityContent);
                        Main.log.info("JSON incoming PUT Request on /reduce");
                        JSONObject jsonStatus=null;
                        //write t1 marker
                        //need refactor
                        if (msgread.getAction().equals(msgread.getAction().REDUCE))                          
                            Main.getMapReduceStat().addMrTimeStat(msgread.getId(), new Date().getTime(), MRTimeStat.TimeMarker.t5);
                        if (msgread.getAction().equals(msgread.getAction().LEADER))
                            Main.getMapReduceStat().addMrTimeStat(msgread.getId(), new Date().getTime(), MRTimeStat.TimeMarker.t7);                                                
                        //TO DO if not action generate error
                        if (msgread.hasAction()){                           
                            jsonStatus=Commands.mapReduce(new RequestMapReduce(msgread));
                            response.setStatusCode(HttpStatus.SC_OK);
                        } else {
                            jsonStatus.put("msg", "Bad request");
                            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                        }
                        response.setStatusCode(HttpStatus.SC_OK);      
                        Main.log.info(jsonStatus.toString()); 
                        StringEntity strEntRequest = new StringEntity (jsonStatus.toString(),System.getProperty("file.encoding"));
                        strEntRequest.setContentType("application/json");     
                        response.setEntity(strEntRequest);
                        return;     
                }
        /*        
        if ((Main.getConfig().isProperty("Security"))){
            if (Main.getConfig().getProperty("Security").equals("2")){
                    if (!Ticket.validateStorageTicket(request)) {
                        response.setStatusCode(HttpStatus.SC_FORBIDDEN);
                        return;
                    }
                }
        }
        final File file = new File(Main.getAgentDocRoot().getPath(), URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));    
        if (file.exists()) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            JSONObject jsonResponse = new JSONObject();
            jsonResponse.put("action", "response");
            jsonResponse.put("status", AgentSystem.STATUS_ERROR);
            jsonResponse.put("msg", "File "+file.getName() +"already exist");
            Main.log.info(jsonResponse.toString());                        
            StringEntity strEntResponse = new StringEntity (jsonResponse.toString());
            strEntResponse.setContentType("application/json");  
            response.setEntity(strEntResponse);
            }     
        Main.log.info("File path to save new file = " + Main.getAgentDocRoot().getPath() + File.separatorChar + file.getName());
        HttpEntity enRequest = ((HttpEntityEnclosingRequest)request).getEntity();        
        InputStream inputstreamEnRequest = enRequest.getContent();
        */
        UploadFileRequest uploadRequest= new UploadFileRequest(request);
        //UploadFileHandler handler=new UploadFileHandler();
        //AgentResponse uploadResponse=handler.handle(uploadRequest);
        AgentResponse uploadResponse=uploadRequest.process();
        uploadResponse.convert(response);
        /*
        OutputStream outFile = new FileOutputStream(Main.getAgentDocRoot().getPath() +File.separatorChar+ file.getName());
        int intBytesRead=0;
        byte[] bytes = new byte[4096];
        while ((intBytesRead=inputstreamEnRequest.read(bytes))!=-1)
            outFile.write(bytes,0,intBytesRead);
            outFile.flush();
        */    
        /*
            //Статус выполнения операции возвращается в ядро
            JSONObject jsonRequest = new JSONObject();
            jsonRequest.put("action", "ack");
            jsonRequest.put("file_size", Long.toString(file.length()));
            jsonRequest.put("id", file.getName());
            jsonRequest.put("agent_ipaddress", Main.getAgentAddress());
            jsonRequest.put("agent_port", Main.getConfig().getProperty("AgentPort"));                  
            try {    
                Network.returnDownloadFileStatus(jsonRequest);
            } catch (Exception ex) {
                Main.log.error("Exception in handle "+ex.getMessage());
            }                                
                //outFile.close();  
            */
    }    
    public static void handleMethodGET(HttpRequest request, HttpResponse response)throws Exception { 
        //final File file = new File(this.docRoot, URLDecoder.decode(request.getRequestLine().getUri(),localEncoding));
        //final File file=null;
        String strFile;
        String strCT="application/octet-stream";
        Main.log.info("docroot = "+Main.getAgentDocRoot().getPath());
        Main.log.info("AgentInetAddress = "+Main.getAgentAddress());
        Main.log.info("URLDecoder en ="+URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS, DELETE");
        //if GET to /agent_status then show json with agent status info
        if (URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).equals("/status")){
            try {
                Network.returnAgentStatus(response);
                return;
            } catch (Exception e) {
                Main.log.error(e.getMessage());
                return;
            }
                 
        } else if(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).startsWith("/command")){
            try {
                AgentRequest commandRequest=RequestFactory.create(request);
                AgentResponse commandResponse=commandRequest.process();
                ((SimpleResponse)commandResponse).convert(response);
                /*
                RequestCommand requestCmd=new RequestCommand(request,response);
                requestCmd.process();
                //init arequest
                */
                return;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }        
        if(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).startsWith("/agent_stat")){
            try {               
                System.out.println("AgentStat request");
                response.setStatusCode(HttpStatus.SC_OK);       
                //System.out.println("AgentStat = "+Main.getMapReduceStat().getStatHtml());
                    StringEntity strEntResponse = new StringEntity (Main.getMapReduceStat().getStatHtml()+Perf.showMemory());
                    //strEntResponse+=(StringEntity)Perf.showMemory();
                    strEntResponse.setContentType("text/html");  
                    response.setEntity(strEntResponse);
                    Main.getMapReduceStat().writeStatToFiles();
                
                return;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
        
        DownloadFileRequest downloadRequest=new DownloadFileRequest(request);
        AgentResponse downloadResponse=downloadRequest.process();
        ((DownloadFileResponse)downloadResponse).convert(response);
        /*
        if(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).contains("?")){
            strCT=(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).
                    split("/?", 2)[1]).split("=", 2)[1];
                        Main.log.info("CT detected = " +strCT);
                        strFile=(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding())).split("\\?",2)[0];
        }
        else {
           strFile=URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()); 
        }
        //Validate ticket
        if ((Main.getConfig().isProperty("Security"))){
        if (Main.getConfig().getProperty("Security").equals("2") &&
                !URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding())
                                .equals("/clientaccesspolicy.xml")){
            if (!Ticket.validateStorageTicket(request)) {
                response.setStatusCode(HttpStatus.SC_FORBIDDEN);
                return;
            }
        }
        }
        //
        final File file = new File(Main.getAgentDocRoot(), strFile);
        if (!file.exists()) {

                    response.setStatusCode(HttpStatus.SC_NOT_FOUND);
                    JSONObject jsonResponse = new JSONObject();
                    jsonResponse.put("action", "response");
                    jsonResponse.put("status", AgentSystem.STATUS_ERROR);
            jsonResponse.put("msg", "File "+file.getName() +" not found");
            Main.log.info(jsonResponse.toString());                        
            StringEntity strEntResponse = new StringEntity (jsonResponse.toString());
            strEntResponse.setContentType("application/json");  
            response.setEntity(strEntResponse);
                 Main.log.error("File " + file.getPath() + " not found");
                
                } else if (!file.canRead() || file.isDirectory()) {
                
                    response.setStatusCode(HttpStatus.SC_FORBIDDEN);
                    JSONObject jsonResponse = new JSONObject();
                    jsonResponse.put("action", "response");
                    jsonResponse.put("status", AgentSystem.STATUS_ERROR);
                    jsonResponse.put("msg", "Access denied");
                    Main.log.info(jsonResponse.toString());                        
                    StringEntity strEntResponse = new StringEntity (jsonResponse.toString());
                    strEntResponse.setContentType("application/json");  
                    response.setEntity(strEntResponse);                
                    } else {
                        if (URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding())
                                .equals("/clientaccesspolicy.xml")) strCT="text/html";
                        response.setStatusCode(HttpStatus.SC_OK);
                        FileEntity body = new FileEntity(file, strCT);
                        //FileEntity body = new FileEntity(file, request.getFirstHeader("Accept").toString());
                        if (request.containsHeader("Accept")){
                            Main.log.info("Request Accept : " + request.getFirstHeader("Accept").getValue());
                        }
                        response.setEntity(body);
                        Main.log.info("Serving file " + file.getPath());
                
                    }    
            */
        }    
    }
