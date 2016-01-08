package itmo.escience.dstorage.agent;
import itmo.escience.dstorage.agent.AgentHttpService;
import itmo.escience.dstorage.agent.RemoteResourceSsh;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.io.*;
import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import org.apache.log4j.*;

import org.w3c.dom.Document.*;


public class Main {
 
    private static SysConfig configProperty;
    public static Logger log = Logger.getLogger(Main.class.getSimpleName()); 
    private static String localEncoding   = System.getProperty("file.encoding");
    private static String strQuota;
    private static String strSsdQuota;
    private static String strMemQuota;
    public static File fAgentDocRoot;
    public static String agentSsdDocRoot;
    public static String sAgentAddress;
    public static String sAgentPort;
    private static PublicKey  publicKey;
    
    private static File javacPath;//="\"c:\\program files\\java\\jdk1.6.0_45\\bin\\javac.exe\"";
    private static File jarPath;//="\"c:\\program files\\java\\jdk1.6.0_45\\bin\\jar.exe\"";
    private static File protocPath;
    //map hierarhy for BigData
    private static Map<String,List<RequestMapReduce>> leaderQueue=new HashMap();
    //private static Map<String,RequestMapReduce> mpleaderReduceQueue;//sessionid:List<RequestMapRedice>
    private static Map<String,List<RequestMapReduce>> reduceQueue=new HashMap();;    
    //private static Map<String,RequestMapReduce> mpagentReduceQueue;
    
    private static MapReduceStat mapreducestat=new MapReduceStat();;
    private static StorageLayer storageLayer;
    //final static String STATUS_ERROR="Error";
    //final static String STATUS_OK="OK";
    //final static boolean isSecure=true;

    public static StorageLayer getStorageLayer(){return storageLayer;}
    
    public static List<RequestMapReduce> getLeaderQueue(String sessionId) {
        if(!leaderQueue.containsKey(sessionId)){
            List<RequestMapReduce> l=new ArrayList();
            leaderQueue.put(sessionId, l);
        }            
        return leaderQueue.get(sessionId);
    }
    public static List<RequestMapReduce> getReduceQueue(String sessionId) {
        if(!reduceQueue.containsKey(sessionId)){
            List<RequestMapReduce> l=new ArrayList();
            reduceQueue.put(sessionId, l);
        }   
        return reduceQueue.get(sessionId);
    }
    public static void delFromReduceQueue(String id){reduceQueue.remove(id);}
    public static void delFromLeaderQueue(String id){leaderQueue.remove(id);}

    //public static void setAgentReduceQueue(List<RequestMapReduce> agentReduceQueue) {
    //    Main.agentReduceQueue = agentReduceQueue;
    //}
    
    public static MapReduceStat getMapReduceStat(){
        return mapreducestat;
    }
    
    public static String getJavacPath(){
        return javacPath.getPath();
        //return "\"c:\\program files\\java\\jdk1.6.0_45\\bin\\javac.exe\"\\";
    }
    
    public static String getJarPath(){
        return jarPath.getPath();
        //return "\"c:\\program files\\java\\jdk1.6.0_45\\bin\\jar.exe\"\\";
    }
    
    public static String getProtocPath(){
        return protocPath.getPath();
        //String s="c:\\DStorage\\protoc.exe";
        //return s;
    }
    
    private static InetAddress AgentInetAddress;
    final static  String agentVersion = "1.0.4";


    final static String defaultRegion ="SPB";
    final static String propertyRegion="region";
    
    
    public static void main(String[] args) throws Exception {
        
        //just test
        /*
        MapReduceMsgProto.MapReduceMsg msgread=null; 
        //InputStreamReader reder=new InputStreamReader(new FileInputStream("C:/temp/csharp-proto.bin"), "UTF8");
        //reder.
        msgread = MapReduceMsgProto.MapReduceMsg.parseFrom(new FileInputStream("C:/temp/csharp-proto.bin"));
        //	msgread = MapReduceMsgProto.MapReduceMsg.parseFrom(new InputStreamReader(new FileInputStream("C:/temp/csharp-proto.bin"), "UTF8")));
        System.out.println("Sff "+msgread.toString());
        
        System.out.println("Sff "+new String(msgread.getData().toByteArray(),"UTF8"));
        System.out.println("Sff "+msgread.getData().toStringUtf8());
        
        if(true)
        return;
        */
        
        
        //Вывод версии клиента
        System.out.println("Starting Storage.Agent.Version "+agentVersion+"\n");
        // Проверка указания конфигурационного файла при запуске агента 
        if (args.length < 1) {
            System.err.println("Please specify path to config file");
            System.exit(1);
        }
        System.out.println("Using config file=" + args[0]);
        // Проверка существования конфигурационного файла        
        File configfile = new File(args[0]);                
        if (!configfile.exists()){
            System.out.println("Config File " + args[0]+ " doesn't exist");
            System.exit(1);  
        }       
        // Set log option from file
        PropertyConfigurator.configure(args[0]);              
        // read config file       
        configProperty = new SysConfig(configfile.getPath());
        
        // Проверка существования необходимых параметров в конфигурационном файле
        if (configProperty.getProperty("AgentdocRoot")==null) {
            log.error("Please specify option AgentdocRoot in config file");
            System.exit(1);
        }
        if (configProperty.getProperty("AgentPort")==null) {
            log.error("Please specify option AgentPort in config file");
            System.exit(1);
        }
        
        if (configProperty.getProperty("AgentAddress")==null) {
            log.error("Please specify option AgentAddress in config file");
            System.exit(1);
        }       
         /*
        try{
            AgentInetAddress=InetAddress.getLocalHost();
            log.info("AgentInetAddress = "+AgentInetAddress.getHostAddress());          
        }catch (Exception e){
            log.error("Exception caught ="+e.getMessage());
        }
        */

        if (configProperty.getProperty("StorageCoreAddress")==null) {
            log.error("Please specify option StorageCoreAddress in config file");
            System.exit(1);
        }
        if (configProperty.getProperty("StorageCorePort")==null) {
            log.error("Please specify option StorageCorePort in config file");
            System.exit(1);
        }        
        log.info("AgentPort="+configProperty.getProperty("AgentPort"));
        log.info("AgentQuota="+configProperty.getProperty("AgentQuota"));
        
        // Проверка не указана ли квота больше чем доступное пространство на диске
        fAgentDocRoot=new File(configProperty.getProperty("AgentdocRoot"));
        agentSsdDocRoot=Main.getConfig().getProperty("AgentSSDdocRoot");
        
        strSsdQuota=Main.getConfig().getProperty("SSDQuota");
        log.info("SSDQuota="+configProperty.getProperty("SSDQuota"));
        strMemQuota=Main.getConfig().getProperty("MEMQuota");
        log.info("MEMQuota="+configProperty.getProperty("MEMQuota"));
        sAgentPort=Main.getConfig().getProperty("AgentPort");
        sAgentAddress=Main.getConfig().getProperty("AgentAddress");
        javacPath=new File(Main.getConfig().getProperty("JavacPath"));
        jarPath=new File (Main.getConfig().getProperty("JarPath"));
        protocPath=new File(Main.getConfig().getProperty("ProtocPath")); 
        Long lgFreeDiskSpace= fAgentDocRoot.getFreeSpace();               
        strQuota = configProperty.getProperty("AgentQuota");
        if (lgFreeDiskSpace.compareTo(new Long(strQuota)) < 0) {
            log.info("FreeDiskSpace :" + lgFreeDiskSpace);
            log.info("AgentQuota :"+ configProperty.getProperty("AgentQuota"));
            log.info("WARNING: Quota larger than available disk space");
        }       
        log.info("Current available quota :" + AgentSystem.returnCurrentQuota());    
        log.info("AgentdocRoot="+ fAgentDocRoot.getPath());
        log.info("StorageCoreAddress="+configProperty.getProperty("StorageCoreAddress"));
        log.info("StorageCorePort="+configProperty.getProperty("StorageCorePort"));
        
        //init Storage Layer
        storageLayer=new StorageLayer();
        storageLayer.setPath(StorageLevel.HDD, fAgentDocRoot.getPath());
        if(agentSsdDocRoot!=null) 
            storageLayer.setPath(StorageLevel.SSD, agentSsdDocRoot);
        storageLayer.setQuota(StorageLevel.HDD, AgentSystem.returnCurrentQuota());
        if(strSsdQuota!=null)
            storageLayer.setQuota(StorageLevel.SSD, Long.parseLong(strSsdQuota));
        if(strMemQuota!=null)
            storageLayer.setQuota(StorageLevel.MEM, Long.parseLong(strMemQuota));
        
        //agentReduceQueue=new ArrayList<RequestMapReduce>();
        //leaderReduceQueue=new ArrayList<RequestMapReduce>();
        //mapreducestat=new MapReduceStat();
        //test variant
        //startTest();
        //testSerializator();
        /*
        String[] fileEntries= (new File(Main.getAgentDocRoot().getPath()+File.separator+"src"+File.separator+"agent")).list();
        for(String f:fileEntries){
            if((new File(Main.getAgentDocRoot().getPath()+File.separator+"src"+File.separator+"agent"+File.separator+f)).delete()){
    			System.out.println(" is deleted!");
    		}else{
    			System.out.println("Delete operation is failed.");
    		}
            
        }
        new File(Main.getAgentDocRoot()+"src"+File.separator+"agent").delete();
        */                        
        // Регистрация агента в ядре
        Network.registerAgent();
        Thread t = new AgentHttpService.DSagentHttpHandler.DSagentRequestListenerThread(Integer.valueOf(
                configProperty.getProperty("AgentPort")),  fAgentDocRoot.getPath());           
        t.setDaemon(false);
        t.start();
    }
    
    
    public static void perfMonitor() throws IOException{
        for (;;){
            Perf.IOActivity();
            Perf.IOQueue();
            Perf.IOSpeed();
        }
    }
    private static String replaceQuotes(String s){
        Main.log.info("s="+s);
        Main.log.info("replace s="+s.replace("\"", ""));
        return s.replace("\"", "");
    }
    
    public static PublicKey getPublicKey() {
        return publicKey;    
    }
    public static void setPublicKey(PublicKey pk) {
        Main.publicKey=pk;    
    }
    public static Logger getLog() {
        return Main.log;
    }
    public static String getAgentAddress(){
        //return Main.AgentInetAddress;
        //return Main.getConfig().getProperty("AgentAddress");
        return sAgentAddress;
    }      
    public static String getAgentPort(){
        //return Main.AgentInetAddress;
        //return Main.getConfig().getProperty("AgentPort");
        return sAgentPort;
    }  
    public static SysConfig getConfig(){
        return Main.configProperty;
    }
    public static String getLocalEncoding() {
        return localEncoding;
    }
    public static String getRegion(){
        if (Main.configProperty.isProperty(Main.propertyRegion))
            return Main.configProperty.getProperty(Main.propertyRegion);
        else
            return Main.defaultRegion;
    }
    public static File getAgentDocRoot() {
        return fAgentDocRoot;
    }

    public static String getQuota() {
        return strQuota;
    }
    public static File getAgentSSDDocRoot() { return new File(agentSsdDocRoot);}
    public static String getSsdQuota() {return strSsdQuota;}
    public static String getMemQuota() {return strMemQuota;}   
    public static String getAgentVersion() {
        return agentVersion;
    }

}
