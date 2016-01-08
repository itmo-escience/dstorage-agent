package itmo.escience.dstorage.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;
import org.json.simple.JSONObject;

public class RemoteResourceCifs {
    //private SmbSession session; 
    private NtlmPasswordAuthentication auth;
    private String addr;
    private String smbpath;

    /**
      *
      * @param strLocalFileName
      * @param strRemoteFileName
      * @param isGet 
      * @throws java.lang.IOException
      */
    public JSONObject copyResource(String strLocalFileName, String strRemoteFileName, boolean isGet) throws IOException{
        JSONObject jsonstatus=new JSONObject();
        try {
            if(isGet){
                final SmbFile smb = new SmbFile(getSmbpath()+strRemoteFileName,auth);      
                InputStream in = new SmbFileInputStream(smb);
                OutputStream outFile = new FileOutputStream(Main.getAgentDocRoot().getPath() +File.separatorChar+ strLocalFileName);
                int intBytesRead=0;
                byte[] bytes = new byte[4096];
                while ((intBytesRead=in.read(bytes))!=-1)
                    outFile.write(bytes,0,intBytesRead);
                outFile.flush();        
                outFile.close();            
            }
            else {
                final SmbFile smb = new SmbFile(getSmbpath()+strRemoteFileName,auth);
                SmbFileOutputStream smbfos = new SmbFileOutputStream(smb);
                InputStream inFile = new FileInputStream(Main.getAgentDocRoot().getPath() +File.separatorChar+ strLocalFileName);           
                int intBytesRead=0;
                byte[] bytes = new byte[4096];
                while ((intBytesRead=inFile.read(bytes))!=-1)
                    smbfos.write(bytes,0,intBytesRead);
                smbfos.flush();
                inFile.close();
            }
        } catch (SmbException e){
            Main.log.info("Error while copy file "+strLocalFileName+" with CIFS remote resource " + addr);
            return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error while copy file. "+e.getLocalizedMessage());
        }
        jsonstatus.put("status", AgentSystem.STATUS_OK);
        return jsonstatus;
    }
       
     /**
      *
      * @param address
      * @param username
      * @param password
      * @throws java.lang.Exception
      */
    public void setAuth(String username, String password, String addr) throws Exception {
        //TODO StringBuilder
        this.auth = new NtlmPasswordAuthentication(username+":"+password);
        this.smbpath="smb://"+addr+"/";
        this.addr=addr;     
    }

    /**
     * 
     * @param path
     * @return 
     * @throws Exception 
     */
    public JSONObject getList(String path) throws Exception {
	List<JSONObject> jsList = new ArrayList<JSONObject>();
        JSONObject jsonResult = new JSONObject();
        SmbFile[] smbfiles = new SmbFile[0];
        if(!path.endsWith("/"))path=path+"/";
        final SmbFile smb=new SmbFile(getSmbpath()+path,auth);  
        try {
            smbfiles = smb.listFiles();
        } catch (SmbException e){
            Main.log.info("Error in retrieve list of directory " + path);
            return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error while list dir. "+e.getLocalizedMessage());
        }            
        for(int i = 0; i < smbfiles.length; i++)
        {
                final JSONObject json = new JSONObject();
                json.put("Name",smbfiles[i].getName());
                json.put("Size",smbfiles[i].length());                
                if (smbfiles[i].isDirectory()) json.put("Type","d");
                else json.put("Type","f");
                jsList.add(json);         
        }
	jsonResult.put("list", jsList);
        jsonResult.put("status", AgentSystem.STATUS_OK);    
	return jsonResult;
    }

    public String getSmbpath() {
        return smbpath;
    }

    public void setSmbpath(String smbpath) {
        this.smbpath = smbpath;
    }
    
    public class RemoteResourceCifsConfig{
        private String pwd;
        private String user;
        private String addr;

        public void setPassword(String pwd) {
            this.pwd = pwd;
        }

        public void setUsername(String user) {
            this.user = user;
        }

        public void setAddress(String addr) {
            this.addr = addr;
        }
    }
}