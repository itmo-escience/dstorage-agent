package itmo.escience.dstorage.agent;

import com.google.protobuf.ByteString;
import itmo.escience.dstorage.agent.MRHandlerParser;
import itmo.escience.dstorage.agent.MapReduceMsgProto.MapReduceMsg;
import itmo.escience.dstorage.agent.MapReduceStat;
import itmo.escience.dstorage.agent.utils.HttpConn;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.entity.InputStreamEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ProcessFile implements Runnable{
//public class ProcessFile{
    
    private String executer;
    private String backURI;
    private String leaderURI;
    private RequestMapReduce request;
    private URLClassLoader urlClassLoader;
    private DefinitionAvgResult method=DefinitionAvgResult.ProcessFile;
    
    public void setBackURI(String backURI) {
        this.backURI = backURI;
    }

    public void setLeaderURI(String leaderURI) {
        this.leaderURI = leaderURI;
    }
    private int count;
    private long session;
    
    ProcessFile(RequestMapReduce request,long session) throws IOException, InterruptedException, Exception{
        this.executer=request.name;
        this.count=request.count;
        this.session=session;
        this.request=request;
        //check that jar file parsed for serialize return value from ProcessFile
        
        if (!(new File(Agent.getAgentDocRoot()+File.separator+request.name+"-MRSerializatorPF.jar")).exists()){
            if(!JarVerifier.verify(executer, DefinitionAvgResult.ProcessFile)) {Agent.log.error(ProcessFile.class.getName()+" Error while verify jar file.");return;}
            //MRHandlerParser parser=new MRHandlerParser(request.name,"bigdata.AvgResult","ProcessFile","MRSerializatorPF");
            MRHandlerParser parser=new MRHandlerParser(request.name,method,"MRSerializatorPF");
            if(!parser.parseJar()) {Agent.log.error(ProcessFile.class.getName()+" Error while parse jar file.");return;}
            parser.writeProto(Agent.getAgentDocRoot()+File.separator+request.name+"-pf.proto");           
            parser.invokeProtoc(Agent.getAgentDocRoot()+File.separator+request.name+"-pf.proto");
            parser.generateSrc();
            parser.generateSerializator();
        }
    }
    @Override
    public void run(){
        //check that jar file exist 
        if (!(new File (Agent.getAgentDocRoot().getPath()+File.separator+this.request.name)).exists()) {            
                Agent.log.error(ProcessFile.class.getName()+". Jar file "+ this.request.name+" doesn't exist");
            try {
                returnResult(null);
            } catch (Exception ex) {
                Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
            }
                return;
        }
        
        //start loop for process files in series
        Iterator iter = request.files.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            //
            long runTime=new Date().getTime();
            
            if ((new File (Agent.getAgentDocRoot().getPath()+File.separator+entry.getKey().toString())).exists())            
                try {
                process(entry.getKey().toString(),entry.getValue().toString());
            } catch (Exception ex) {
                Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
            }
            else {
                Agent.log.error(ProcessFile.class.getName()+" . File "+ entry.getKey().toString()+"doesn't exist");
                try {
                    returnResult(null);
                } catch (Exception ex) {
                    Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
                }
                continue;   
                }                           
            Agent.getMapReduceStat().addStat(new MapStat(new Date(),Agent.getAgentAddress(),Long.toString(session),entry.getKey().toString(),((new Date().getTime())-runTime)));
                   
           /*
            try {
                Perf.IOActivity();
                Perf.IOQueue();
                Perf.IOSpeed(); 
            } catch (IOException ex) {
                Agent.log.error(ProcessFile.class.getName()+" . Error while execute perf task");
            }
            */
        }
    } 
    private void process(String file,String storagefilename) throws Exception{
        Object res=null;
        URL url,url2,url3; 
        try {
            url = new URL("file:"+Agent.getAgentDocRoot().getPath()+File.separator+this.request.name);
            url3 = new URL("file:"+Agent.getAgentDocRoot()+File.separator+this.executer+"-MRSerializatorPF.jar");
            urlClassLoader = new URLClassLoader (new URL[] {url,url3});
            Class<?> cl = Class.forName ("bigdata.AvgResult", true, urlClassLoader);
            Method join = cl.getMethod("ProcessFile", new Class[]{InputStream.class,String.class,String.class});
            res=(join.invoke(cl.newInstance(),new FileInputStream(new File(Agent.getAgentDocRoot().getPath()+File.separator+file)),storagefilename, request.options ));
            //Agent.log.info("res="+res.getClass().getName());                                
        } catch (MalformedURLException ex) {
            Agent.log.error(ProcessFile.class.getName()+" . MalformedURLException "+ex);
        } catch (ClassNotFoundException ex) {
            Agent.log.error(ProcessFile.class.getName()+" . ClassNotFoundException "+ex);
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Agent.log.error(ProcessFile.class.getName()+" . NoSuchMethodException "+ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Agent.log.error("File "+file+" not found");           
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        }
        returnResult(res);
    }
    private void returnResult(Object res) throws Exception{        
        
        Agent.log.error("Object in returnResult "+res.toString());
        //create MapReduceMsg         
        MapReduceMsg.Builder mbuilder=MapReduceMsg.newBuilder();
        mbuilder.setAction(MapReduceMsg.Action.REDUCE);
        mbuilder.setId(request.task_id);
        mbuilder.setBackUri(request.backURI);
        mbuilder.setOptions(request.options);
        mbuilder.setLeaderUri(request.leaderURI);
        mbuilder.setCount(count);
        mbuilder.setName(executer);
        mbuilder.setAgentList(request.agentList.toString());
        mbuilder.setSessionId(Long.toString(session));
        mbuilder.setDatafile(request.datafile);
        mbuilder.setProto(request.proto);
        //mbuilder.setData(ByteString.copyFrom(objectToByte(res)));
        mbuilder.setData(ByteString.copyFrom(serializeResult(res)));
        
        Agent.getMapReduceStat().addMrTimeStat(request.task_id, new Date().getTime(), MRTimeStat.TimeMarker.t4);
        try{
            HttpConn httpconn = new HttpConn();
            httpconn.setup(Agent.getAgentAddress(),Agent.getAgentPort()); 
            httpconn.setMethod("PUT","/reduce");
            //httpconn.setEntity(mapRes);
            //mbuilder.build().toByteArray()
            //mbuilder.build().
            httpconn.setEntity(new InputStreamEntity(new ByteArrayInputStream(mbuilder.build().toByteArray()),-1));
            httpconn.connect();
            httpconn.close();
        } catch (UnknownHostException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        }        
        //return mapRes;
    }
    private byte[] serializeResult(Object ob){
        byte[] bytes=null;
        try {
            Class<?> cl=urlClassLoader.loadClass("agent.MRSerializator");
            Method getSerial = cl.getMethod("getSerialized", new Class<?>[] {Object.class});
            bytes=(byte[])getSerial.invoke(cl.newInstance(),ob);
        } catch (ClassNotFoundException ex) {
            Agent.log.error(ProcessFile.class.getName()+" . ClassNotFoundException "+ex);            
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Agent.log.error(ProcessFile.class.getName()+" . NoSuchMethodException "+ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(ProcessFile.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return bytes;
        
    }
    private byte[] objectToByte(Object ob) throws Exception { 
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(ob);
        return b.toByteArray();        
    }
}
