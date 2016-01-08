package itmo.escience.dstorage.agent.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author anton
 */
public class AgentProperties {
    private String filename="agent.config.properties";
    private Properties prop=null;
    public AgentProperties(){init();}
    public AgentProperties(String filename){
        this.filename=filename;
        init();
    }
    public void init(){
        File file=new File(this.filename);
        if(!file.exists()) {//create new properies file
            prop=new Properties();
            OutputStream os=null;
            try {
                os=new FileOutputStream(filename);
                prop.setProperty("size", "0");
                this.prop.store(os, null);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(AgentProperties.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(AgentProperties.class.getName()).log(Level.SEVERE, null, ex);
            } finally{
                if(os!=null){
                    try {
			os.close();
                        this.prop=null;
			} catch (IOException e) {
				e.printStackTrace();
			}
                }
            }
        }
    }
    public List<String> retrieve(){
        List<String> result=new ArrayList();
        InputStream is=null;
        try {
            is = new FileInputStream(filename);
            prop=new Properties();
            prop.load(is);
            for(String key:prop.stringPropertyNames()){
                if(key.contains("size"))continue;//omit size key
                result.add(prop.getProperty(key));
            }    
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AgentProperties.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(AgentProperties.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                Logger.getLogger(AgentProperties.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return result;
    }
    public void add(String val){
        int size=Integer.parseInt(this.prop.getProperty("size"));
        prop.setProperty(Integer.toString(size), val);
        prop.setProperty("size", Integer.toString(size+1));
        save();        
    }
    public void replace(Set<String> list){
        prop.clear();
        int i=0;
        for(String s:list)
            prop.setProperty(Integer.toString(i), s);
        prop.setProperty("size", Integer.toString(list.size()));
        save();
    }    
    public void save () {
	OutputStream output = null;
	try {
            output = new FileOutputStream(filename);
            prop.store(output, null);

	} catch (IOException io) {
		io.printStackTrace();
	} finally {
		if (output != null) {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
    }
}
