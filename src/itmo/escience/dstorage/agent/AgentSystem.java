package itmo.escience.dstorage.agent;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AgentSystem {

    final static String STATUS_ERROR="Error";
    final static String STATUS_OK="OK";    
    final static boolean isSecure=true;
    final static boolean isGet=true;    
    
    public static Long returnCurrentQuota(){
        // Получение количества свободного места с учетом квоты и места в разделе
        Long lgCurrentQuota;
        // Место в разделе
        Long lgFreeDiskSpace= Main.getAgentDocRoot().getFreeSpace();
        // Квота в конфигурационном файле
        //Long lgQuota=Long.valueOf(strQuota);
        // Размер директории для хранения файлов                
        lgCurrentQuota=Long.valueOf(Main.getQuota())-getDocRootSize(Main.getAgentDocRoot());
        //Другие процессы занимают место в разделе под файлы
        if (lgCurrentQuota.compareTo(lgFreeDiskSpace)>0)
            lgCurrentQuota=lgFreeDiskSpace;
        //Квота превышена
        if (lgCurrentQuota < 0){
            lgCurrentQuota = Long.valueOf(0) ;
        }
        return lgCurrentQuota;
    }
    public static Long returnSsdCurrentQuota(){
        // Получение количества свободного места с учетом квоты и места в разделе
        Long lgCurrentQuota;
        // Место в разделе
        Long lgFreeDiskSpace= (new File (Main.agentSsdDocRoot)).getFreeSpace();
        // Квота в конфигурационном файле
        //Long lgQuota=Long.valueOf(strQuota);
        // Размер директории для хранения файлов                
        lgCurrentQuota=Long.valueOf(Main.getSsdQuota())-getDocRootSize(new File(Main.agentSsdDocRoot));
        //Другие процессы занимают место в разделе под файлы
        if (lgCurrentQuota.compareTo(lgFreeDiskSpace)>0)
            lgCurrentQuota=lgFreeDiskSpace;
        //Квота превышена
        if (lgCurrentQuota < 0){
            lgCurrentQuota = Long.valueOf(0) ;
        }
        return lgCurrentQuota;
    }
        public static String getStringFromInputStream (InputStream inputStream) throws IOException {        
        StringBuilder strBuffer = new StringBuilder();
        byte[] bBuffer = new byte[4096];
        for (int n; (n = inputStream.read(bBuffer)) != -1;) {
            strBuffer.append(new String(bBuffer, 0, n));
        }
        return strBuffer.toString();
}         
        public static JSONObject parseJSON(String obj) {             
         JSONParser jsonParser = new JSONParser();
         JSONObject jsonObject = new JSONObject();
         try {
             jsonObject = (JSONObject)jsonParser.parse(obj);
         } 
         catch (ParseException ex) {
            Main.log.error("JSON Parse Error. Object:"+ex.getUnexpectedObject()+"; Position:"+ex.getPosition());
            return jsonObject; 
         }
         catch (Exception ex) {                  
            Main.log.error("JSON Exception:"+ex.getMessage());
            return jsonObject; 
         }          
        return jsonObject;
    }        
    public static long getDocRootSize(File fDirectory) {
        long lgDocRootLength = 0;
        for (File file : fDirectory.listFiles()) {
            if (file.isFile())
                lgDocRootLength += file.length();
            else
                lgDocRootLength += getDocRootSize(file);
        }
        return lgDocRootLength;
    }
    public static JSONObject createMsgStatus(String code, String reply){
        JSONObject jsonResult=new JSONObject();        
        jsonResult.put("status", code);
        jsonResult.put("msg", reply);
        return jsonResult;        
    }
    public static boolean isIPv4Address(String str) { 
        final String IP_PATTERN = 
		"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
		"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
      Pattern ipPattern = Pattern.compile(IP_PATTERN);  
      Matcher matcher = ipPattern.matcher(str);
      return matcher.matches();  
    }
    public static byte[] objectToByte(Object ob) throws Exception { 
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
        ObjectOutput objectOut = null;
        objectOut = new ObjectOutputStream(baos);   
        objectOut.writeObject(ob);
        byte[] bytes = baos.toByteArray();
        baos.close();
        return bytes; 
    }
}
