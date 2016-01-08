package itmo.escience.dstorage.agent;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
 
public class SysConfig 
{
   Properties configFile;
   public SysConfig(String fileName)
   {
	configFile = new Properties();
	try {
          InputStream is = new FileInputStream(fileName);
          configFile.load(is);
	}
        catch(Exception ex){
            System.out.print(ex.getLocalizedMessage());
	}
   }
 
   // получение значение параметра
   public String getProperty(String key)
   {
	String value = this.configFile.getProperty(key);
	return value;
   }
   
   public void printPropertyList() {
    configFile.list(System.out);
  }
   public boolean isProperty(String key)
   {
	return (!(this.configFile.getProperty(key)==null || this.configFile.getProperty(key).isEmpty()));  
   }
   public Map getMapType() {

       Map <String,String> ctMap = new HashMap <String,String>();
       String str = "";
       int delimiter =-1; 
       Iterator it = configFile.entrySet().iterator();
       while (it.hasNext()) {
           str = it.next().toString();
           delimiter = str.indexOf("=");
           if (delimiter>0) {
            ctMap.put(str.substring(0, delimiter), str.substring(delimiter+1));
           }
       }
       return ctMap;
  }
}