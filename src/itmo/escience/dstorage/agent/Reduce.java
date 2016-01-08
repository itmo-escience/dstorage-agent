package itmo.escience.dstorage.agent;

import com.google.protobuf.ByteString;
import itmo.escience.dstorage.agent.MapReduceMsgProto.MapReduceMsg;
import itmo.escience.dstorage.agent.RequestMapReduce.MapReduceRequestType;
import itmo.escience.dstorage.agent.utils.HttpConn;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.json.simple.JSONObject;
//import agent.MRHandlerParser;

public class Reduce implements Runnable{
    private URI nextAdr;
    private int id;
    private int amount;
    private String executer;
    private URLClassLoader urlClassLoader;
    RequestMapReduce request;
    public ByteArrayOutputStream boutstream;
    public Object[] aobj;
    private List<Object> aObj;
    
    private DefinitionAvgResult method=DefinitionAvgResult.JoinResults;
    public ByteArrayOutputStream getOutstream() {
        return boutstream;
    }
    
    Reduce(RequestMapReduce request) throws IOException, InterruptedException, Exception{
        this.request=request;
        boutstream=new ByteArrayOutputStream();
        //Object[] arrayOb;
        this.aObj=new ArrayList<Object>();
        //check that jar file parsed for reduce
        
        if (!(new File(Agent.getAgentDocRoot()+File.separator+request.name+"-MRSerializatorJR.jar")).exists()){
            
            if(!JarVerifier.verify(request.name, DefinitionAvgResult.JoinResults)) {Agent.log.error(Reduce.class.getName()+" Error while verify jar file.");return;}
            MRHandlerParser parser=new MRHandlerParser(request.name,method,"MRSerializatorJR");
            if(!parser.parseJar()) {Agent.log.error(Reduce.class.getName()+" Error while parse jar file.");return;}
            parser.writeProto(Agent.getAgentDocRoot()+File.separator+request.proto);            
            parser.invokeProtoc(Agent.getAgentDocRoot()+File.separator+request.proto);
            parser.generateSrc();
            parser.generateSerializator();
        }
        URL url,url2;
        url = new URL("file:"+Agent.getAgentDocRoot().getPath()+File.separator+this.request.name);
        url2 = new URL("file:"+Agent.getAgentDocRoot()+File.separator+this.request.name+"-MRSerializatorJR.jar");
        urlClassLoader = new URLClassLoader (new URL[] {url,url2});        
    }
    public void addObject(byte[] data){
        this.aObj.add(deSerializeInput(data));      
    }
    @Override
    public void run() {
        Object res=null;
        //Object[] arrayOb={};        
        try {

            Class<?> cl = Class.forName (method.getJarClassName(), true, urlClassLoader);
            Agent.log.info("Object class="+aObj.size());
            //Method join = cl.getMethod(method.getMethodName(), new Class[]{Object[].class});
            Method join = cl.getMethod(method.getMethodName(), method.getMethodParameters());
            
            res=(join.invoke(cl.newInstance(),new Object[]{this.aObj.toArray()}));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        }             
        MapReduceMsgProto.MapReduceMsg.Builder mbuilder=MapReduceMsgProto.MapReduceMsg.newBuilder();        
        mbuilder.setId(request.task_id);
        mbuilder.setBackUri(request.backURI);
        mbuilder.setOptions(request.options);
        mbuilder.setLeaderUri(request.leaderURI);
        mbuilder.setAgentList(request.agentList.toJSONString());
        mbuilder.setSessionId(request.sessionID);
        mbuilder.setName(request.name);
        mbuilder.setCount(request.count);     
        mbuilder.setDatafile(request.datafile);
        mbuilder.setProto(request.proto);
         //mbuilder.setData(ByteString.copyFrom(AgentSystem.objectToByte(res))); 
         mbuilder.setData(ByteString.copyFrom(serializeResult(res)));
        try{
            HttpConn httpconn = new HttpConn();
            
            //check that it's final reduce
            switch (request.type) {
                case REDUCE:
                    mbuilder.setAction(MapReduceMsgProto.MapReduceMsg.Action.LEADER);
                    MapReduceMsg msg = mbuilder.build();
                    httpconn.setup(request.leaderURI.split(":")[0],request.leaderURI.split(":")[1]); 
                    httpconn.setMethod("PUT","/reduce");
                    httpconn.setEntity(new ByteArrayEntity(msg.toByteArray()));
                    break;
                case LEADER:
                    //TODO change to protobuf result write to file
                    writeDataFile(request.datafile,serializeResult(res));
                    //WORKAROUND for repeated jar usage
                    if(!(new File(Agent.getAgentDocRoot()+File.separator+request.proto)).exists())
                        writeProto(Agent.getAgentDocRoot()+File.separator+request.proto);
                    //check that proto file exist if not create
                    //writeProto(request.proto);
                    JSONObject ackjson=new JSONObject();
                    ackjson.put("action","ack");
                    ackjson.put("map_reduce", "true");
                    ackjson.put("agent_ipaddress", Agent.getAgentAddress());
                    ackjson.put("id", request.task_id);                   
                    //reduceRes=ackjson;
                    httpconn.setup(request.backURI.split(":")[0],request.backURI.split(":")[1]);  
                    httpconn.setMethod("PUT","/map");
                    httpconn.setEntity(ackjson);
                    break;
            }
            if (request.type==MapReduceRequestType.REDUCE)
                Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t6);
            else 
                Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t8);
            httpconn.connect();
            httpconn.close();
            
        } catch (UnknownHostException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            //Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    private void writeDataFile(String file, byte[] data) throws FileNotFoundException, IOException{
        InputStream is = new ByteArrayInputStream(data);
        OutputStream outFile = new FileOutputStream(Agent.getAgentDocRoot().getPath() +File.separatorChar+ file);
        int intBytesRead=0;
        byte[] bytes = new byte[4096];
        while ((intBytesRead=is.read(bytes))!=-1)
            outFile.write(bytes,0,intBytesRead);
        outFile.flush();
        outFile.close();
        is.close();
    }
    private void writeProto(String protoname ){

    }
    public byte[] serializeResult(Object ob){
        byte[] bytes=null;
        try {
            Class<?> cl=urlClassLoader.loadClass("agent.MRSerializator");
            Method getSerial = cl.getMethod("getSerialized", new Class<?>[] {Object.class});
            bytes=(byte[])getSerial.invoke(cl.newInstance(),ob);            
        } catch (ClassNotFoundException ex) {
            Agent.log.error(Reduce.class.getName()+" . ClassNotFoundException "+ex);
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Agent.log.error(Reduce.class.getName()+" . NoSuchMethodException "+ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return bytes;
        
    }
    public Object deSerializeInput(byte[] data){
        byte[] bytes=null;
        Object res=null;
        try {
            Class<?> cl = Class.forName ("agent.MRSerializator", true, urlClassLoader);
            Method getSerial = cl.getMethod("getDeserialized", new Class<?>[] {byte[].class});
            res=getSerial.invoke(cl.newInstance(),data);                       
        } catch (ClassNotFoundException ex) {
            Agent.log.error(Reduce.class.getName()+" . ClassNotFoundException "+ex);
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Agent.log.error(Reduce.class.getName()+" . NoSuchMethodException "+ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(Reduce.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return res;
        
    }
}
