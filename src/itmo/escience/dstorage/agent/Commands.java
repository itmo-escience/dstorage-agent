package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.utils.RemoteResourceType;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Commands {
    public enum RemoteResourceCommandType {LIST, EXEC, COPYTO, COPYFROM };
    private final static String STATUS_OK="OK";
    private final static String STATUS_ERROR="error";
    //Old interface by GET method
    public static void AgentURLCommand(HttpRequest request, 
            HttpResponse response) throws Exception {
        JSONObject jsonRequest= new JSONObject();
        String target = request.getRequestLine().getUri();
        Agent.log.info("URLDecoder = "+target);
        String[] strOption;
        strOption=target.split("[/]");      
        Agent.log.info("action "+(strOption[2].split(":"))[1]);
        Agent.log.info("agent_address "+(strOption[3].split(":"))[1]);
        Agent.log.info("agent_port "+(strOption[4].split(":"))[1]);
        Agent.log.info("file_id "+(strOption[5].split(":"))[1]); 
        if ((strOption[2].split(":"))[1].equals("get"))
            {
            try {
                if ((Agent.getConfig().isProperty("Security"))){
                    if (Agent.getConfig().getProperty("Security").equals("2")){
                        if (!Ticket.validateStorageTicket(request)) return;
                        jsonRequest.put("Ticket",request.getFirstHeader("Ticket").getValue());
                        jsonRequest.put("Sign",request.getFirstHeader("Sign").getValue());                    
                    }
                }                                          
                URL urlAgent= new URL ("http://"+(strOption[3].split(":"))[1]+":"+((strOption[4].split(":"))[1]));
                Agent.log.info("URL "+urlAgent.toString());
                Network.doGetFileFromAgent(strOption[5].split(":")[1], urlAgent, jsonRequest);
                JSONObject jsonStatus = new JSONObject();
                jsonStatus.put("status", STATUS_OK);
                jsonStatus.put("msg", "Command completed");
                response.setStatusCode(HttpStatus.SC_OK);
                Agent.log.info(jsonStatus.toString());                        
                StringEntity strEntRequest = new StringEntity (jsonStatus.toString());
                strEntRequest.setContentType("application/json");     
                response.setEntity(strEntRequest);         
            }
            catch (MalformedURLException e) {
                JSONObject jsonStatus = new JSONObject();
                jsonStatus.put("status", STATUS_ERROR);
                jsonStatus.put("msg", "Invalid command");
                response.setStatusCode(HttpStatus.SC_OK);
                Agent.log.info(jsonStatus.toString());                        
                StringEntity strEntRequest = new StringEntity (jsonStatus.toString());
                strEntRequest.setContentType("application/json");     
                response.setEntity(strEntRequest);
                Agent.log.info(e.getLocalizedMessage());
                return;
            }       
        } else {
            Agent.log.info("Unsupported action in command : "+target);
            return;
        } 
}
    
    public static JSONObject zip(JSONObject jsonPutRequest) throws Exception {
        JSONObject jsonStatus=new JSONObject();
        JSONObject jsonList=null;
        //JSONObject jsonTmp;
        //Agent.log.info("zipArchive = " + jsonPutRequest);
        jsonList=(JSONObject)jsonPutRequest.get("list");
        //Agent.log.info("zipArchive List = " + jsonList);
        String zipfile=Agent.getAgentDocRoot().getPath() + File.separatorChar+jsonPutRequest.get("archive").toString();
        String rootDir=Agent.getAgentDocRoot().getPath() + File.separatorChar;
        //Agent.log.info("rootDir = " + rootDir);
        //Agent.log.info("zipfile = " + zipfile);
        //check that files and zipfile exist
        if((new File(zipfile)).exists()){
            jsonStatus.put("msg", "Archive file already exist");
            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
            return jsonStatus;
        }
        Iterator iter = jsonList.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next(); 
            //omit emty dirs
            if (entry.getKey().equals("dirEntry")) continue;
            if(!(new File(rootDir+entry.getKey().toString())).exists()){
                Agent.log.info("filename = " + entry.getKey().toString());
                jsonStatus.put("msg", "At least one archived file doesn't exist");
                jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                return jsonStatus;
            }
        }
            /*
        for (int i=0;i<jsonList.size();i++){
            if(!(new File(rootDir+((JSONObject)jsonList.get(i)).get("file").toString())).exists()){
                Agent.log.info("filename = " + ((JSONObject)jsonList.get(i)).get("file"));
                jsonStatus.put("msg", "At least one archived files doesn't exist");
                jsonStatus.put("status", AgentSystem.STATUS_ERROR);
                return jsonStatus;
            }
        }  
        * */
        Thread th=new Thread(new ZipArchive(jsonList,zipfile,rootDir));
        th.start();
        jsonStatus.put("msg", "Command to zip was accepted");
        jsonStatus.put("status", AgentSystem.STATUS_OK);
        Agent.log.info("jsonStatus="+jsonStatus.toString());
        return jsonStatus;
        /*
        try {
        ZipOutputStream zipOutStream =
              new ZipOutputStream(new FileOutputStream(zipfile));
         for (int i=0;i<jsonList.size();i++)
         {
            //Agent.log.info("jsonTMp = " + jsonTmp.toString());
            jsonTmp=(JSONObject)jsonList.get(i);
            FileInputStream fileInputStream = new FileInputStream(rootDir+
                    ((JSONObject)jsonList.get(i)).get("file"));
            zipOutStream.putNextEntry(new ZipEntry(((JSONObject)jsonList.get(i)).get("file").toString()));        
               int intBytesRead=0;
               byte[] bytes = new byte[4096];
                while ((intBytesRead=fileInputStream.read(bytes))!=-1)
                zipOutStream.write(bytes,0,intBytesRead);
                fileInputStream.close();
                           
         }
         zipOutStream.flush(); 
         zipOutStream.close(); 
         } catch (FileNotFoundException fx) {
            Agent.log.error("FileNotFoundException :"+fx.getLocalizedMessage());
            jsonStatus.put("msg", "ZipArchive. File not found");
            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
         } catch (IOException ioex) {                  
            Agent.log.error("IOexception:"+ioex.getMessage());
            jsonStatus.put("msg", "Input-Output Error");
            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
         }   
        jsonStatus.put("msg", "Archive "+jsonPutRequest.get("archive").toString()+ " succesful created");
        jsonStatus.put("status", AgentSystem.STATUS_OK);
        return jsonStatus;
        */
        
    }
    public static JSONObject parseCommmand(JSONObject jsonPutRequest){        
        JSONObject jsonStatus=null;
        return jsonStatus;
    }       
    public static JSONObject mapReduce(RequestMapReduce request) throws IOException, InterruptedException, Exception{
        //PrintWriter pwriter2 = new PrintWriter(new BufferedWriter(new FileWriter("MapLogAv_"+Agent.getAgentAddress()+".log", true)));
        //PrintWriter pwriterReduce = new PrintWriter(new BufferedWriter(new FileWriter("MapLogReduce_"+Agent.getAgentAddress()+".log", true)));
        //PrintWriter pwriterLeader = new PrintWriter(new BufferedWriter(new FileWriter("MapLogLeader_"+Agent.getAgentAddress()+".log", true)));
        
        //RequestMapReduce request  = new RequestMapReduce(jsonMapReduce.toString());
        switch (request.type) {
            case MAP: 
                //System.out.println("Start map!!!");
                long sessionID=Calendar.getInstance().getTime().getTime();
                final ProcessFile pf=new ProcessFile(request,sessionID);
                pf.setBackURI(request.backURI);
                pf.setLeaderURI(request.leaderURI);
                //pf.isParsed();
                long runTime=new Date().getTime();
                /////////////////
                Thread th=new Thread(pf);
                th.start();
                ///////////////////////
                //pf.run();    
                //WRONG METHOD
                Agent.getMapReduceStat().addStat(new MapTotalStat(new Date(),Agent.getAgentAddress(),((new Date().getTime())-runTime),request.task_id));
                Process pFlush=Runtime.getRuntime().exec("cmd /c \"C:/temp/EmptyStandbyList.exe\"");
                //Thread.sleep(1000);
                //Perf.IOActivity();
                //Perf.IOQueue();
                //Perf.IOSpeed();        
                break;
            case REDUCE:
                Agent.log.info("Start reduce!!!");
                //Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t3);
                long runTimeReduce=new Date().getTime();
                //Object[] aobj;
                Agent.getReduceQueue(request.sessionID).add(request);
                    //int count=0;
                    //for (int n=0;n<Agent.getAgentReduceQueue().size();n++){
                    //    if (request.sessionID.equals(Agent.getAgentReduceQueue().get(n).sessionID))
                    //        count++;
                    if(request.count==Agent.getReduceQueue(request.sessionID).size()) {
                        //start reduce
                        Reduce reduce=new Reduce(request);
                        //aobj=new Object[count]; 
                        //int z=0;
                        /*
                        for (int i=0;i<Agent.getAgentReduceQueue().size();i++){
                            if (Agent.getAgentReduceQueue().get(i).sessionID.equals(request.sessionID)){
                                System.out.println("Add data N="+i);
                                reduce.addObject(Agent.getAgentReduceQueue().get(i).data);
                            }                         
                        }
                        */
                        for(RequestMapReduce rmr:Agent.getReduceQueue(request.sessionID))reduce.addObject(rmr.data);
                        Agent.getReduceQueue(request.sessionID).clear();
                        Agent.delFromReduceQueue(request.sessionID);
                        Thread thr=new Thread(reduce);
                        thr.start();
                        //reduce.start();                        
                        //Agent.getAgentReduceQueue().clear();
                        //Agent.getMapReduceStat().addStat(new ReduceStat(new Date(),Agent.getAgentAddress(),((new Date().getTime())-runTimeReduce),request.task_id));
                        Agent.getMapReduceStat().addStat(new ReduceStat(new Date(),Agent.getAgentAddress(),((new Date().getTime())-runTimeReduce),request.task_id));
                        //sent mrtimestat to core
                        if (!(request.leaderURI.split(":")[0].equals(Agent.getAgentAddress())&& request.leaderURI.split(":")[1].equals(Agent.getAgentPort())) )
                            Agent.getMapReduceStat().sendMrTimeStat(Agent.getConfig().getProperty("StorageCoreAddress"),Agent.getConfig().getProperty("StorageCorePort"), reduce.request.task_id);
                        break;
                         }                    
                        
                //Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t6);
                        
                //pwriterReduce.append("reduce "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date()))+" "+Agent.getAgentAddress()+" "+request.sessionID+" "+request.count+" "+Long.toString((new Date().getTime())-runTimeReduce));
                //pwriterReduce.println();
                    
                break;
            case LEADER:
                long runTimeLeader=new Date().getTime();
                Agent.getLeaderQueue(request.task_id).add(request);
                //System.out.println("Start leader-reduce!!!");
                Agent.log.info("LeaderReduceQueue="+Agent.getLeaderQueue(request.task_id).size());                                
                //check that all agent return reduce results
                /*int c=0;
                for (int i=0;i<Agent.getLeaderReduceQueue().size();i++){
                    //calculate count of entry with the same agentList
                    if (request.task_id.equals(Agent.getLeaderReduceQueue().get(i).task_id))
                        c++;
                    */
                    //check that agentCount the same as should be to finish reduce and return result to core
                    //if (c==100){
                    if (request.agentCount==Agent.getLeaderQueue(request.task_id).size()){
                        //start reduce process on leader
                        Reduce reduce=new Reduce(request);
                        
                        //aobj=new Object[c];
                        //int z=0;
                        //join data from all agents
                        /*
                        for (int n=0;n<Agent.getLeaderReduceQueue().size();n++){
                            if (Agent.getLeaderReduceQueue().get(n).task_id.equals(request.task_id)){
                                reduce.addObject(Agent.getLeaderReduceQueue().get(i).data);
                                
                            }
                        }
                        */
                        for(RequestMapReduce rmr:Agent.getLeaderQueue(request.task_id)){
                            reduce.addObject(rmr.data);
                        }        
                        reduce.run();                       
                        Agent.getLeaderQueue(request.task_id).clear();
                        Agent.delFromLeaderQueue(request.task_id);
                        Agent.getMapReduceStat().addStat(new ReduceLeaderStat(new Date(),Agent.getAgentAddress(),((new Date().getTime())-runTimeLeader),request.task_id));
                        //sent mrtimestat to core
                        Agent.getMapReduceStat().sendMrTimeStat(Agent.getConfig().getProperty("StorageCoreAddress"),Agent.getConfig().getProperty("StorageCorePort"), reduce.request.task_id);
                        
                        break;
                    }
                //}
                //pwriterLeader.append("leader "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date()))+" "+Agent.getAgentAddress()+" "+request.sessionID+" "+request.count+" "+Long.toString((new Date().getTime())-runTimeLeader));
                //pwriterLeader.println();
                //Thread.sleep(1000);
                //Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t8);
                
                Agent.getMapReduceStat().writeStatToFiles();
                break;
        }        
        JSONObject jsonStatus=new JSONObject();
        jsonStatus.put("msg", "Command to mapReduce was accepted");
        jsonStatus.put("status", AgentSystem.STATUS_OK);
        Agent.log.info("jsonStatus="+jsonStatus.toString());
        return jsonStatus;
    }
   
    public static JSONObject handleRRCommand(JSONObject jsonPutRequest) throws Exception{
        JSONObject jsonStatus=new JSONObject();
        RemoteResourceType remoteResourceRequest;
        if (!jsonPutRequest.containsKey("user") || !jsonPutRequest.containsKey("pwd") || 
                !jsonPutRequest.containsKey("host") || !jsonPutRequest.containsKey("type") ||
                        !jsonPutRequest.containsKey("name")){
            return AgentSystem.createMsgStatus(STATUS_ERROR, "Not enought fields in json request. Omitted mandatoty field user/pwd/host/type/name");
        }
        ////!!!!!!!!!!!!!!
        try {
            remoteResourceRequest=RemoteResourceType.valueOf(jsonPutRequest.get("type").toString().toUpperCase());
        } catch (Exception e){
            return AgentSystem.createMsgStatus(STATUS_ERROR, "Field type not supported. Supported list are SSH,FTP,FTPS,CIFS");  
        }
        //RemoteResourceCommandType remoteResourceCommandRequest=
        //                    RemoteResourceCommandType.valueOf(jsonPutRequest.get("name").toString().toUpperCase());
        if(jsonPutRequest.get("name").toString().toUpperCase().equals("LIST") && !jsonPutRequest.containsKey("path")){
            return AgentSystem.createMsgStatus(STATUS_ERROR, "Not enought fields in json request. Omitted mandatoty field path");
        }
        if(jsonPutRequest.get("name").toString().toUpperCase().equals("EXEC") && !jsonPutRequest.containsKey("cmd")){
            return AgentSystem.createMsgStatus(STATUS_ERROR, "Not enought fields in json request. Omitted mandatoty field cmd");
        }
        if((jsonPutRequest.get("name").toString().toUpperCase().equals("COPYTO") || jsonPutRequest.get("name").toString().toUpperCase().equals("COPYFROM"))
                && (!jsonPutRequest.containsKey("localfile") || !jsonPutRequest.containsKey("remotefile"))){
            return AgentSystem.createMsgStatus(STATUS_ERROR, "Not enought fields in json request. Omitted mandatoty field localfile/remotefile");
        }
/*
        //only list command in sync mode 
        if(jsonPutRequest.get("name").toString().toUpperCase().equals("LIST")||
                jsonPutRequest.get("name").toString().toUpperCase().equals("EXEC")){
         switch(remoteResourceRequest) {
                case SSH:
                    RemoteResourceSsh ssh = new RemoteResourceSsh();          
                    ssh.setConfig(jsonPutRequest.get("user").toString(), jsonPutRequest.get("pwd").toString(),
                    InetAddress.getByName(jsonPutRequest.get("host").toString()));
                    final JSONObject json=ssh.connectResource();
                    if (json.get("status").equals(AgentSystem.STATUS_ERROR)){
                        jsonStatus=json;
                        break;
                        }
                    if(jsonPutRequest.get("name").toString().toUpperCase().equals("LIST")){
                        jsonStatus=ssh.getList(jsonPutRequest.get("path").toString());
                    }
                    else{
                        jsonStatus=ssh.execCommand(jsonPutRequest.get("cmd").toString());
                    }
                    break;
                case CIFS:
                    RemoteResourceCifs cifs = new RemoteResourceCifs(); 
                    cifs.setAuth(jsonPutRequest.get("user").toString(),
                    jsonPutRequest.get("pwd").toString(), 
                    jsonPutRequest.get("host").toString());
                    jsonStatus=cifs.getList(jsonPutRequest.get("path").toString());
                    break;
                case FTP:
                    RemoteResourceFtp ftp = new RemoteResourceFtp(); 
                    ftp.setConfig(jsonPutRequest.get("user").toString(),
                    jsonPutRequest.get("pwd").toString(), 
                    jsonPutRequest.get("host").toString());   
                    jsonStatus=ftp.getList(jsonPutRequest.get("path").toString(),false);
                    break;
                case FTPS:
                    RemoteResourceFtp ftps = new RemoteResourceFtp(); 
                    ftps.setConfig(jsonPutRequest.get("user").toString(),
                    jsonPutRequest.get("pwd").toString(), 
                    jsonPutRequest.get("host").toString());
                    jsonStatus=ftps.getList(jsonPutRequest.get("path").toString(),true);
                    break;            
        }
        return jsonStatus; 
        }
        */
        //async mode
        if (!jsonPutRequest.containsKey("mode")) jsonPutRequest.put("mode","sync");
        if ( jsonPutRequest.get("mode").toString().toUpperCase().equals("ASYNC")){
            try{
                Thread th=new Thread(new RRExecuter(remoteResourceRequest,jsonPutRequest,true));
                th.start();
                jsonStatus=AgentSystem.createMsgStatus(STATUS_OK, "Command successful accepted");
            }catch (Exception ex){
                jsonStatus=AgentSystem.createMsgStatus(STATUS_ERROR, "Error while handle command "+ex.getLocalizedMessage());
            }
            Agent.log.info("jsonStatus="+jsonStatus.toString());    
            return jsonStatus;
        }
        else{
            RRExecuter rr= new RRExecuter(remoteResourceRequest,jsonPutRequest,false);
            rr.run();
            return rr.getStatus();
        }
    }
    /*
    public JSONObject CommandSsh(JSONObject jsonPutRequest) throws Exception{
        
    }
    public JSONObject CommandCifs(JSONObject jsonPutRequest) throws Exception{
        
    }
    public JSONObject CommandFtps(JSONObject jsonPutRequest) throws Exception{
        
    }
    */
}
