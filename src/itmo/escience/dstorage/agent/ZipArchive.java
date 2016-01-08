package itmo.escience.dstorage.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 *
 * 
 */
public class ZipArchive implements Runnable {
    //private JSONArray jsonList;
    private JSONObject list;
    private String zipfile,rootDir;
    @Override
    public void run() {   
        JSONObject jsonStatus=new JSONObject();
        jsonStatus.put("action", "ack");
        jsonStatus.put("agent_ipaddress", Main.getAgentAddress());
        jsonStatus.put("agent_port", Main.getConfig().getProperty("AgentPort"));
        try {
            ZipOutputStream zipOutStream =
            new ZipOutputStream(new FileOutputStream(zipfile));
            //for (int i=0;i<jsonList.size();i++)
            Iterator iter = list.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry entry = (Map.Entry)iter.next();               
                //handle empty dir
                if (entry.getKey().toString().equals("dirEntry")) {
                    //TODO parser with exception
                    JSONArray jlist=(JSONArray)list.get("dirEntry");

                    for(int i=jlist.size()-1;i>=0;i--){
                        String strTmp;
                        strTmp=jlist.get(i).toString();
                        if(!strTmp.endsWith("/"))strTmp=strTmp+File.separatorChar;
                        zipOutStream.putNextEntry(new ZipEntry(strTmp));
                    }
                    continue;
                    
                }
                FileInputStream fileInputStream = new FileInputStream(rootDir+entry.getKey());
                zipOutStream.putNextEntry(new ZipEntry(entry.getValue().toString()));                              
                int intBytesRead=0;
                byte[] bytes = new byte[4096];
                while ((intBytesRead=fileInputStream.read(bytes))!=-1)
                    zipOutStream.write(bytes,0,intBytesRead);
                fileInputStream.close();                           
            }
        zipOutStream.flush(); 
        zipOutStream.close(); 
        jsonStatus.put("msg", "Archive "+(new File(zipfile)).getName()+ " created");
        jsonStatus.put("status", AgentSystem.STATUS_OK);
        jsonStatus.put("file_id",(new File(zipfile)).getName());
        jsonStatus.put("file_size",Long.toString((new File(zipfile)).length()));
        } catch (FileNotFoundException fx) {
            Main.log.error("FileNotFoundException :"+fx.getLocalizedMessage());
            jsonStatus.put("msg", "ZipArchive. File not found");
            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
        } catch (IOException ioex) {                  
            Main.log.error("IOexception:"+ioex.getMessage());
            jsonStatus.put("msg", "Input-Output Error");
            jsonStatus.put("status", AgentSystem.STATUS_ERROR);
        } 
        try{
            Network.returnDownloadFileStatus(jsonStatus);
        }catch (Exception ex){
            Main.log.error("Exception :"+ex.getLocalizedMessage());
        }
    }
    public ZipArchive(JSONObject list, String zipfile, String rootDir) {
        this.list=list;
        this.rootDir=rootDir;
        this.zipfile=zipfile;        
    }
    
}
