package itmo.escience.dstorage.agent;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.net.ftp.*;
import org.json.simple.JSONObject;

public class RemoteResourceFtp {
    
    private RemoteResourceFtpConfig ftpConfig;
    //final private static String STATUS_ERROR="Error";
    //final private static String STATUS_OK="OK";
    
    public RemoteResourceFtp(){
       ftpConfig=new RemoteResourceFtpConfig();
    }
    
    public void setConfig (String user, String pwd, String addr){
        this.ftpConfig.addr=addr;
        this.ftpConfig.pwd=pwd;
        this.ftpConfig.user=user;        
    }
    public String getAddr(){
        return this.ftpConfig.addr;
    }
    public String getUser(){
        return this.ftpConfig.user;
    }
    public String getPwd(){
        return this.ftpConfig.pwd;
    }
    
    /**
     * Method to setup FTP connection
     * 
     * @param ftpclient
     * @return JSONObject
     * @throws Exception 
     */
    
    private JSONObject setupFtpConnection(FTPClient ftpclient) throws Exception{
        JSONObject jsonResult = new JSONObject();
        try {           
            ftpclient.connect(getAddr());
            Main.log.info("Connected to Ftp Remote Resource " + getAddr() + ".");
            if(!FTPReply.isPositiveCompletion(ftpclient.getReplyCode())) {         
                Main.log.info("FTP server refused connection.");
                jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,ftpclient.getReplyString());
                ftpclient.disconnect();
                return jsonResult;
            }                     
            ftpclient.login(getUser(),getPwd());
            //status 230 	User logged in, proceed. 
            //This status code appears after the client sends the correct password. It indicates that the user has successfully logged on. 
            if(ftpclient.getReplyCode()!=230) {                
                Main.log.info("FTP server doesn't accept username/password.");
                jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,ftpclient.getReplyString());
                ftpclient.disconnect();
                return jsonResult;               
            }         
            ftpclient.setControlEncoding(Main.getLocalEncoding());
            ftpclient.setFileType(FTP.BINARY_FILE_TYPE);
            //ftpclient.setFileTransferMode(FTP.BINARY_FILE_TYPE);
            ftpclient.enterLocalPassiveMode(); 
        } catch(IOException ioe) {
           //ioe.printStackTrace();
           Main.log.info("Error communicating with FTP Remote Resource " +getAddr());
           jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error communicate FTP Resource. "+ioe.getLocalizedMessage());
           ftpclient.disconnect();
        } 
        return AgentSystem.createMsgStatus(AgentSystem.STATUS_OK,ftpclient.getReplyString());
    }    
    
    /** Method for copy files to Remote Resource by FTP protocol
     * 
     * @param strLocalFile
     * @param strRemoteFile
     * @param isGet
     * @param isSecure
     * @return 
     */
    public JSONObject copyResource(String strLocalFile, String strRemoteFile, boolean isGet, boolean isSecure) throws Exception {
        JSONObject jsonResult = new JSONObject();
        FTPClient ftpclient;
        if (!isSecure) {
            ftpclient = new FTPClient();
        }
        else  {
            FTPSClient ftpsclient = new FTPSClient();
            ftpclient=ftpsclient;
        }
        try {  
            jsonResult=setupFtpConnection(ftpclient);
            if (jsonResult.get("status").equals(AgentSystem.STATUS_ERROR))
                return jsonResult;
            if(isGet){
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream
                        (Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFile));
                ftpclient.retrieveFile(strRemoteFile,outputStream);
                outputStream.close();
            }
            else {
                //InputStream inStream = new BufferedInputStream(new FileInputStream
                //        (Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFile));
                InputStream inFile = new FileInputStream(Main.getAgentDocRoot().getPath()+File.separatorChar+strLocalFile);
                ftpclient.storeFile(strRemoteFile,inFile);
                inFile.close();
            }          
            //Определение статуса выполненной операции
            if (ftpclient.getReplyCode()<200 || ftpclient.getReplyCode()>=300 )
                 jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,ftpclient.getReplyString());
            else jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_OK,ftpclient.getReplyString());              
            ftpclient.logout();
            ftpclient.disconnect();
        } catch(IOException ioe) {
           ioe.printStackTrace();
            Main.log.info("Error communicating with FTP Remote Resource " +getAddr());
            jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error communicating with FTP Resource");
            ftpclient.disconnect();
        } 
        return jsonResult;
    }

    /**
     * Get list objects from directory by FTP protocol
     * 
     * @param path 
     * @return 
     */
    public JSONObject getList(String path, boolean isSecure) throws Exception {
        JSONObject jsonResult = new JSONObject();
        List<JSONObject> List = new ArrayList<JSONObject>();               
        FTPClient ftpclient;
        if (isSecure) 
            ftpclient = new FTPSClient();
        else  ftpclient=new FTPClient();              
        try {
            jsonResult=setupFtpConnection(ftpclient);
            if (jsonResult.get("status").equals(AgentSystem.STATUS_ERROR))
                return jsonResult;
            ftpclient.changeWorkingDirectory(path);
            if (ftpclient.getReplyCode()==550){
            return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,ftpclient.getReplyString());
            }
        } catch(IOException ioe) {
            Main.log.info(ioe.getLocalizedMessage());
            Main.log.info("Error communicating with FTP Remote Resource " +getAddr());
            return AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,"Error communicating with FTP Resource");
        } 
        FTPFile[] ftpfiles = ftpclient.listFiles(path); 
        if (ftpclient.getReplyCode()<200 && ftpclient.getReplyCode()>=300 )
            jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_ERROR,ftpclient.getReplyString());
        else 
            jsonResult=AgentSystem.createMsgStatus(AgentSystem.STATUS_OK,ftpclient.getReplyString());  
        for(int i = 0; i < ftpfiles.length; i++)
                {
                final JSONObject json = new JSONObject();
                json.put("Name",ftpfiles[i].getName()); 
                //String utf8String= (ftpfiles[i].getName()).getBytes("windows-1251");
                //String utf8= utf8String.getBytes("UTF-8");                
                if (ftpfiles[i].isDirectory()) json.put("Type","d");
                else json.put("Type","f");
                json.put("Size",ftpfiles[i].getSize()); 
                List.add(json);             
                }
        jsonResult.put("list", List);
        Main.log.info("JSONresult = "+jsonResult.toString());
        return jsonResult;
    }
    
    /**
     * @param 
     */
    public class RemoteResourceFtpConfig{
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
