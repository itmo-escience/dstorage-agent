package itmo.escience.dstorage.agent;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Anton Spivak
 */
public class JarVerifier {
    public static boolean verify(String file,DefinitionAvgResult armethod){
        URL url;
        Class<?> cl=null;//bigdata.AvgResult
        Method m=null;
        try {
            url = new URL("file:"+Main.fAgentDocRoot.getPath()+File.separator+File.separator+file);
        } catch (MalformedURLException ex) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error while load URL:"+"file:"+Main.fAgentDocRoot.getPath()+File.separator+File.separator+file, ex);
            return false;
        }
        URLClassLoader loader = new URLClassLoader (new URL[] {url});
        try{
            cl = Class.forName (armethod.getJarClassName(), true, loader);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error while load class "+armethod.getJarClassName()+" from jar file "+
                    file, ex);
            return false;
        }                
        try {
            //Check signature of Method
            m=cl.getMethod(armethod.getMethodName(),armethod.getMethodParameters());
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error while get method "+armethod.getMethodName()+" from jar file "+
                    file, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error while get method "+armethod.getMethodName()+" from jar file "+
                    file, ex);
        } catch (Exception ex) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error while get method "+armethod.getMethodName()+" from jar file "+
                    file, ex);
        }
        if(m==null) {
            Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Error verification jar file:"+file+"Method "+armethod.getMethodName()+" with signature "+
                    armethod.getStrMethodParameters());
            return false;
        }
        //check returned type from method
        //is standard type
        if(armethod.isStdType(m.getReturnType()))
            return true;
        else{
            Main.log.info("Find Custom type return value "+ m.getReturnType().getCanonicalName());
            //Check that custom type exist in jar
            /*
            boolean found=false;
            for(Class<?> allcl:cl.getClasses()){
                Main.log.info("Class "+allcl.getName()+" "+m.getReturnType().getCanonicalName());
                if(allcl.getName().equals(m.getReturnType().getCanonicalName())){                    
                    found=true;
                }
            }
            if(!found){Main.log.info("Jar parse:Inner class "+m.getReturnType().getName()+" not found");return false;}
            */
            Class<?> incl=null;
            try {
                incl = Class.forName (m.getReturnType().getName(), true, loader);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(JarVerifier.class.getName()).log(Level.SEVERE, "Couldn't load class "+m.getReturnType().getName(), ex);
                return false;
            }
            //Agent.log.info("in "+incl.getName().toString());
            //if(incl==null){Main.log.info("Inner Class Name "+m.getReturnType().getCanonicalName()+" load is fail");return false;}
            //for(String t:incl.getName().toString().split("\\."))
            //    Main.log.info("in split "+t);
                //Agent.log.info("in split "+incl.getName().toString().split("\\.").toString());
            
            String innerClass=incl.getName().split("\\.")[1];
            Main.log.info("Inner Class Name is "+innerClass);
            for(Field fl: incl.getDeclaredFields()){ 
                //check that all fields of inner class is standard
                if(!armethod.isStdType(fl.getType())){
                    Main.log.info("Inner Class Name "+innerClass+" has wrong type of inner field "+fl.getName()+" :"+fl.getType());
                    return false;
                }
            }
            //check that inner clas has empty constructor 
            boolean found=false;
            for(Constructor<?> c:incl.getConstructors())
                if(c.getParameterTypes().length==0) return true;
            if(!found){Main.log.info("Jar parse:Inner class "+m.getReturnType().getName()+" doesn't have default constructor");}
        }
        return false;
    }
    
}
