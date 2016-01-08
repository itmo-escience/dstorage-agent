package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.utils.AgentMessage;
import itmo.escience.dstorage.agent.utils.AgentMessageCreater;
import itmo.escience.dstorage.agent.utils.AgentProperties;
import itmo.escience.dstorage.agent.utils.AgentSystemStatus;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;

/**
 *
 * @author anton
 */
public class StorageLayer {

    private final Map<StorageLevel,Long> quotas=new HashMap();
    private final Map<StorageLevel,String> paths=new HashMap();
    private long memUsage=0;
    private final ConcurrentHashMap<String,ByteBuffer> mappedFiles=new ConcurrentHashMap<String,ByteBuffer>();
    private final ConcurrentHashMap<String,FileChannel> mappedFCFiles=new ConcurrentHashMap<String,FileChannel>();
    //private ConcurrentHashMap<String,String> ssdFiles=new ConcurrentHashMap<String,String>();
    private AgentProperties agentProp;
    public StorageLayer(){
        agentProp=new AgentProperties();
        for(String mpfile:agentProp.retrieve())
            mapFileToMem(new File(fileToHddPath(mpfile)));
        Agent.log.info("Load to memory:"+mappedFiles.keySet());
    }
    public void setPath(StorageLevel type,String path){paths.put(type, path);}
    public void setQuota(StorageLevel type,long size){quotas.put(type, size);}
    //public int getSsdFileCount(){return ssdFiles.size();}
    public int getMappedFileCount(){return mappedFiles.size();}
    public boolean isFileExist(String filename){ //check that file exist on hdd and ssd
        File f=new File(fileToHddPath(filename));
        if(f.exists()) return true;
        f=new File(fileToSsdPath(filename));
        if(f.exists()) return true;        
        return false;    
    }
    public boolean isFileOnLevel(StorageLevel level,String id){
        switch(level){
            case HDD:return isHddStorage(id);
            case MEM:return isMemStorage(id);
            case SSD:return isSsdStorage(id);
            default: return false;
        }
    }
    public boolean isFileAccessed(String filename){ //check that file can be get from fs
        File fhdd=new File(fileToHddPath(filename));
        File fssd=new File(fileToSsdPath(filename));
        if((fhdd.canRead() && !fhdd.isDirectory()) || (fssd.canRead() && !fssd.isDirectory())) return true;        
        return false;    
    }    
    public long addFile(StorageLevel type,InputStream is,String filename){
        long size=0;
        switch(type){
            case NOTSET: size=addToDisk(is,fileToHddPath(filename)); break;
            case HDD: size=addToDisk(is,fileToHddPath(filename)); break;
            case SSD: size=addToDisk(is,fileToSsdPath(filename));break;
            case MEM: 
                if(!isFileOnLevel(StorageLevel.HDD,filename)){
                    size=addToDisk(is,fileToHddPath(filename));
                    if(size==0) return size;
                }
                fileToMem(new File(fileToHddPath(filename)));
                //save in properties file 
                agentProp.add(filename);
                break;
        }
        
        return size;
    }
    public long getFreeMemToUse(){
        long quota=Long.parseLong(Agent.getMemQuota());
        //if((quota-memUsage)<0) return 0;
        return quota-memUsage>0?quota-memUsage:0;
    }
    public boolean deleteFile(String filename){
        //System.out.println("delete map file:"+filename+" l:"+filename.length());
        if(mappedFiles.containsKey(filename)){
            /*
            MappedByteBuffer buf=mappedFiles.get(filename);
            FileChannel fc=mappedFCFiles.get(filename);
            
            mappedFiles.remove(filename);
            mappedFCFiles.remove(filename);
            try {
                fc.close();
                unmap(fc,buf);
            } catch (Exception ex) {
                Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, "Exception while unmap file ", ex);
            }
            */
            mappedFiles.remove(filename);
            memUsage-=(new File(fileToHddPath(filename))).length();
            //remove from properties file
            agentProp.replace(mappedFiles.keySet());
        }
        if(isSsdStorage(filename)) {
            File file=new File(fileToSsdPath(filename));
            return file.delete();
        }
        File file=new File(fileToHddPath(filename));
        return file.delete();
    }
    public boolean deleteFileFromLevel(StorageLevel level,String filename){
        switch(level){
            case HDD:
                File filehdd=new File(fileToHddPath(filename));
                return filehdd.delete();
            case MEM:
                if(mappedFiles.containsKey(filename)){     
                mappedFiles.remove(filename);
                memUsage-=(new File(fileToHddPath(filename))).length();
                //remove from properties file
                agentProp.replace(mappedFiles.keySet());
                return true;
                }
                else return false;
            case SSD:
                if(isSsdStorage(filename)) {
                    File file=new File(fileToSsdPath(filename));
                    return file.delete();
                }else return false;
        }
        return false;
    }
    private boolean isSsdStorage(String filename){
        File file=new File(fileToSsdPath(filename));
        if(file.exists()  && file.canRead() && !file.isDirectory()) return true;
        return false;
    }
    private boolean isHddStorage(String filename){
        File file=new File(fileToHddPath(filename));
        if(file.exists()  && file.canRead() && !file.isDirectory()) return true;
        return false;
    }
    private boolean isMemStorage(String filename){
        return mappedFiles.containsKey(filename);
    }
    public ByteArrayEntity getFile(StorageLevel type,String filename,ContentType contentType) {
        switch(type){
            case MEM: return getFileFromMEM(filename,contentType);                
            case SSD: return getFileFromDiskAsBAE(fileToSsdPath(filename),contentType);
            case HDD: return getFileFromDiskAsBAE(fileToHddPath(filename),contentType);
            case NOTSET: return getFileFromBestStorage(filename,contentType);
            default: {
                return getEntityWithErrorMsg();
            }
        }
    }
    public long getFileLen(StorageLevel type,String filename){
        switch(type){
            case MEM: 
                if(!mappedFiles.containsKey(filename)){
                    return 0;
                }
                ByteBuffer bb=this.mappedFiles.get(filename);
                return bb.array().length;              
            case SSD: 
                return new File(fileToHddPath(filename)).length();
            case HDD: 
                return new File(fileToHddPath(filename)).length();
            default: 
                return 0;            
        }
    }
    public InputStream getFileAsStream(StorageLevel type,String filename) {
        switch(type){
            case MEM: return getFileFromMEMStream(filename);                
            case SSD: return getFileFromDiskAsStream(fileToSsdPath(filename));
            case HDD: return getFileFromDiskAsStream(fileToHddPath(filename));
            //case NOTSET: return getFileFromBestStorage(filename,contentType);
            default: {
                return null;
            }
        }
    }
    public static ByteArrayEntity getEntityWithErrorMsg(){
        return new ByteArrayEntity((AgentMessageCreater.createJsonError(AgentMessage.NOTFOUND.getString(), AgentSystemStatus.FAILED)).getBytes(),
                        ContentType.APPLICATION_JSON);
    }
    private ByteArrayEntity getFileFromMEM(String filename,ContentType contentType){ 
        if(!mappedFiles.containsKey(filename)){
            return getEntityWithErrorMsg();
        }
        else{
            //MappedByteBuffer mapped=readMappedFile(filename);
            ByteBuffer mapped=readFileInMem(filename);
            mapped.position(0);
            byte[] bytes=null;
            //if(mapped.isLoaded()){Agent.log.info("Mapped loaded");}
            if(mapped.hasArray()){
                Agent.log.info("Mapped direct");
                bytes=mapped.array();
            }
            else{
                Agent.log.info("Mapped indirect:"+mapped.capacity());
                bytes=new byte[mapped.capacity()];
                mapped.get(bytes);
            }
            ByteArrayEntity ent=new ByteArrayEntity(bytes,contentType);
            return ent;      
        }
    }
    
    private InputStream getFileFromMEMStream(String filename){ 
        if(!mappedFiles.containsKey(filename)){
            return null;
        }
        else{
            ByteBuffer mapped=readFileInMem(filename);
            mapped.position(0);
            byte[] bytes=null;
            if(mapped.hasArray()){
                Agent.log.info("Mapped direct");
                bytes=mapped.array();
            }
            else{
                Agent.log.info("Mapped indirect:"+mapped.capacity());
                bytes=new byte[mapped.capacity()];
                mapped.get(bytes);
            }
            ByteArrayInputStream ent=new ByteArrayInputStream(bytes);
            return ent;      
        }
    }
    
    public ByteArrayEntity getFileFromBestStorage(String filename,ContentType contentType) {
        if(mappedFiles.containsKey(filename)){
            return getFileFromMEM(filename,contentType);
        }
        if(isSsdStorage(filename)) return getFileFromDiskAsBAE(fileToSsdPath(filename),contentType);        
        return getFileFromDiskAsBAE(fileToHddPath(filename),contentType);
    }
    public StorageLevel getFileStorageType(String filename){
        if(isFileOnLevel(StorageLevel.MEM,filename)) return StorageLevel.MEM;
        
        if(isFileOnLevel(StorageLevel.SSD,filename)) return StorageLevel.SSD;

        return StorageLevel.HDD;
    }
    private static ByteArrayEntity getFileFromDiskAsBAE(String filename,ContentType contentType){
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        try {
            InputStream is=new FileInputStream(new File(filename));
            byte[] buf =new byte[4096];
            int intBytesRead=0;
            byte[] bytes = new byte[4096];
            while ((intBytesRead=is.read(buf))!=-1)
                baos.write(bytes,0,intBytesRead);
            baos.flush();
            is.close();
            } catch (FileNotFoundException ex) {
                Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
                return getEntityWithErrorMsg();
            } catch (IOException ex) {
                Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
                return getEntityWithErrorMsg();                
            }
        return new ByteArrayEntity(baos.toByteArray(),contentType);
    }
    private static InputStream getFileFromDiskAsStream(String filename){
        InputStream is=null;
        try {
            is=new FileInputStream(new File(filename));            
            } catch (FileNotFoundException ex) {
                Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);                
            } 
        return is;
    }
    private static String fileToSsdPath(String filename){
        return Agent.getAgentSSDDocRoot().getPath()+File.separatorChar+ filename;
    }
    private static String fileToHddPath(String filename){
        return Agent.getAgentDocRoot().getPath() +File.separatorChar+ filename;
    }
    private long addToDisk(InputStream is,String path){
        OutputStream outStream;             
        try {
            outStream = new FileOutputStream(path);        
            int intBytesRead=0;
            byte[] bytes = new byte[4096];
            while ((intBytesRead=is.read(bytes))!=-1)
                outStream.write(bytes,0,intBytesRead);
            outStream.flush();
            outStream.close();
            //is.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
        }           
        return (new File(path)).length();
    }
    private boolean mapFileToMem(File file){
        //check that quota not exceeded
        //System.out.println(Agent.getMemQuota());
        if(memUsage>Long.parseLong(Agent.getMemQuota()))return false;
        try {
            FileChannel fileChannel=new RandomAccessFile(file,"r").getChannel();
            MappedByteBuffer fileInMem=fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
            fileInMem.load();
            this.mappedFiles.put(file.getName(), fileInMem);
            this.mappedFCFiles.put(file.getName(), fileChannel);
            memUsage+=file.length();
            //Agent.log.info("Loaded:"+fileInMem.isLoaded());
        } catch (IOException ex) {
            Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, "File to map doesn't exist in filesystem:"+file.getPath(), ex);
            return false;
        }       
        return true;
    }
    private boolean fileToMem(File file){
        int bytesRead=-1;
        try {
            if(memUsage>Long.parseLong(Agent.getMemQuota()))return false;
            FileChannel fileChannel=new RandomAccessFile(file,"r").getChannel();
            ByteBuffer buf = ByteBuffer.allocate((int)file.length());
            bytesRead = fileChannel.read(buf);
            this.mappedFiles.put(file.getName(), buf);
            //this.mappedFCFiles.put(file.getName(), fileChannel);
            memUsage+=file.length();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(StorageLayer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return bytesRead!=-1?true:false;
    }
    /*
    private MappedByteBuffer readMappedFile(String filename){
        //System.out.println("read mapped file: "+filename);
        return this.mappedFiles.get(filename);
    }
    */
    private ByteBuffer readFileInMem(String filename){
        //System.out.println("read mapped file: "+filename);
        return this.mappedFiles.get(filename);
    }
    private static void unmap(FileChannel fc, MappedByteBuffer bb) throws Exception {
        Class<?> fcClass = fc.getClass();
        java.lang.reflect.Method unmapMethod = fcClass.getDeclaredMethod("unmap",new Class[]{java.nio.MappedByteBuffer.class});
        unmapMethod.setAccessible(true);
        unmapMethod.invoke(null,new Object[]{bb});
    }
    public static long getTotalPhisicalMemory(){
        com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        return os.getTotalPhysicalMemorySize();
    }
}
