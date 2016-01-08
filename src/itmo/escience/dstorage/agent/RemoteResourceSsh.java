package itmo.escience.dstorage.agent;

import java.net.InetAddress;
import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSchException;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Vector;
import org.json.simple.JSONObject;

public class RemoteResourceSsh {
   private RemoteResourceConfig configSession;
   private Session session;
   private Channel channel;
   private final static int SSH_DEFAULT_PORT=22;
   
   public RemoteResourceSsh(){
       configSession=new RemoteResourceConfig();
   }
   
   public void setConfig (String user, String pwd, InetAddress addr){
        this.configSession.setAddr(addr);
        this.configSession.setPwd(pwd);
        this.configSession.setUser(user);        
   }
   public InetAddress getAddr(){
       return this.configSession.getAddr();
   }
   public String getUser(){
       return this.configSession.getUser();
   }
   public String getPwd(){
       return this.configSession.getPwd();
   }
   
   public JSONObject connectResource(){
       try {
           JSch jsch = new JSch();
           session = jsch.getSession(configSession.getUser(), configSession.getAddr().getHostAddress(), SSH_DEFAULT_PORT);
           java.util.Properties config = new java.util.Properties();
           config.put("StrictHostKeyChecking", "no");
           session.setConfig(config);
           session.setPassword(configSession.getPwd());
           session.connect();
        } catch (JSchException jsche) {
            Main.log.info("Error communicating with SSH Remote Resource " +configSession.getAddr().getHostAddress());
            Main.log.info("SSH connection error "+jsche.getLocalizedMessage());
            return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,jsche.getLocalizedMessage());
        }
        return AgentSystem.createMsgStatus(AgentSystem.STATUS_OK,"");
   }
   public JSONObject copyResource(String strLocalFileName, String strRemoteFileName, boolean isGet) throws IOException{          
       JSONObject jsonResult= new JSONObject();
       //CommandResult cmdResult=new CommandResult();
       try {
            Channel channel = session.openChannel("sftp");
            channel.connect();     
            try {
                ChannelSftp sftpChannel = (ChannelSftp) channel;
                if (isGet){
                    sftpChannel.get(strRemoteFileName, Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFileName);
                    jsonResult.put("file_size",Long.toString((new File(Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFileName)).length()));
                } else {
                    File fLocal = new File(Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFileName);
                    sftpChannel.put(new FileInputStream(fLocal), strRemoteFileName);                    
                }
                jsonResult.put("file_id",strLocalFileName);
                //cmdResult.statusCode=Integer.toString(sftpChannel.getExitStatus());
                sftpChannel.exit();
            } catch (SftpException e) {
                Main.log.info("Error while copy file with SSH Resource " +configSession.getAddr().getHostAddress());
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error while copy file "+e.getLocalizedMessage());
            }
                      
       } catch (JSchException e) {
                Main.log.info("Error communicating with SSH Remote Resource " +configSession.getAddr().getHostAddress());
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,e.getLocalizedMessage());
            } finally {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
       }
       jsonResult.put("status", AgentSystem.STATUS_OK);
       return jsonResult;         
   }
   public JSONObject getList(String strRemotePath) throws IOException{
       JSONObject jsonResult=new JSONObject();
       try {
            try{
                //check that directory exist             
                final ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
                sftpChannel.connect();
                sftpChannel.cd(strRemotePath);
            } catch (SftpException e) {
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Directory doesn't exist");
            }
            try {
                final ChannelSftp sftpChannel = (ChannelSftp)session.openChannel("sftp");
                sftpChannel.connect();
                Vector filelist = sftpChannel.ls(strRemotePath);
                List<JSONObject> listJson=new ArrayList<JSONObject>();
                for(int i=0; i<filelist.size();i++){
                    final JSONObject json = new JSONObject();
                    final LsEntry lsEntry=(LsEntry)filelist.get(i);
                    if (lsEntry.getFilename().equals(".")||lsEntry.getFilename().equals("..")) continue;                
                    json.put("Name", lsEntry.getFilename());
                    if (lsEntry.getAttrs().isDir()) json.put("Type","d");
                    else json.put("Type","f");    
                    json.put("Size", lsEntry.getAttrs().getSize());
                    listJson.add(json);  
                }
                jsonResult.put("list", listJson);
                jsonResult.put("status", AgentSystem.STATUS_OK);
            } catch (SftpException e) {
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error while list directory");
            } 
            } catch (JSchException e) {
                Main.log.info("Error communicating with Ssh Remote Resource " +configSession.getAddr().getHostAddress());
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,e.getLocalizedMessage());
            } finally {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
            }
       return jsonResult;
   }
   public JSONObject execCommand(String strCommand) throws IOException  {     
        CommandResult cmdResult=new CommandResult();
        BufferedReader bufferedReader=null;
        BufferedReader bufferedReaderEr=null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            ((ChannelExec)channel).setCommand(strCommand);
            channel.setInputStream(null);
            
            InputStream stdout = channel.getInputStream();
            InputStream stderr = channel.getExtInputStream();
            channel.connect();
            bufferedReaderEr = new BufferedReader(new InputStreamReader(stderr));
            bufferedReader = new BufferedReader(new InputStreamReader(stdout));
            String buffer=null, result="";
            String bufferEr=null, resultEr="";
            while ((buffer = bufferedReader.readLine()) != null) {
                result = result + " "+ buffer;
            }
            while ((bufferEr = bufferedReaderEr.readLine()) != null) {
                resultEr = resultEr + " "+ bufferEr;
            }
            cmdResult.statusCode=Integer.toString(channel.getExitStatus());
            if(cmdResult.statusCode.equals("0"))
                cmdResult.strCommandOutput=result;
            else 
                cmdResult.strCommandOutput=resultEr;
            } catch (JSchException e) {
                Main.log.info("Error communicating with Ssh Remote Resource " +configSession.getAddr().getHostName());
                return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,e.getLocalizedMessage());
            } finally {
                if (channel != null) {
                    channel.disconnect();
                }
                if (session != null) {
                    session.disconnect();
                }
            }
        JSONObject jsonResult=new JSONObject();
        String status;
        if(cmdResult.statusCode.equals("0"))
            status="OK";
        else status="error";                   
        jsonResult.put("msg", cmdResult.strCommandOutput);
        jsonResult.put("status", status);
        return jsonResult;
   }   
}
class RemoteResourceConfig {
       private String user;
       private String pwd;
       private InetAddress addr;

    public void setUser(String user) {
        this.user = user;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public void setAddr(InetAddress addr) {
        this.addr = addr;
    }

    public String getUser() {
        return user;
    }

    public String getPwd() {
        return pwd;
    }

    public InetAddress getAddr() {
        return addr;
    }           
}
class CommandResult {
    String strCommandOutput=null;
    String statusCode="1";
}
