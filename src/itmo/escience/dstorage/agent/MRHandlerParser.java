package itmo.escience.dstorage.agent;

import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Anton Spivak
 */
public class MRHandlerParser {
    private Map<String,ProtoType> proto;
    private Map<String,String> javaproto;
    private enum ValueType {STRING, INT, BOOLEAN,DOUBLE,FLOAT,EMPTY,BYTESTRING,OBJECT };
    private String protoName;
    private enum ProtoType {STRING, INT32, BOOL,DOUBLE,FLOAT,BYTES,EMPTY };
    private String classname; 
    private String innerClass; 
    private String file;
    private boolean isStdType=false;
    private String stdType=null;
    //private String methodIn="ProcessFile"; 
    private String method;     
    private String serializatorName;
    private String methodRet="JoinResults"; 
    private static String PACKAGE_NAME="agent"; 
    private static String SERIALIZATOR_CLASSNAME="MRSerializator";
    private static String SERIALIZATOR_PF="MRSerializatorPF";
    private static String SERIALIZATOR_JR="MRSerializatorJR";
    private final String currentPath;
    private final DefinitionAvgResult defReq;
    
    MRHandlerParser(String file,DefinitionAvgResult met,String serializatorName) throws IOException{
         
        this.proto=new HashMap<String,ProtoType>();
        this.javaproto=new HashMap<String,String>();
        this.classname=met.getJarClassName();
        this.method=met.getMethodName();
        this.serializatorName=serializatorName;    
        this.file=file;
        this.currentPath=Main.fAgentDocRoot.getPath()+File.separator;  
        this.defReq=met;

    }
    public boolean parseJar() throws Exception{
        URL url;
        try {
            Method m;
            url = new URL("file:"+currentPath+File.separator+file);
            URLClassLoader loader = new URLClassLoader (new URL[] {url});
            Class<?> cl = Class.forName (classname, true, loader);
            //TODO change condition          
            //Check signature of ProcessFile Method
            /*
            for(Method mm:cl.getMethods()){
                if(mm.getName().equals(MRMethodName.ProcessFile.toString())) {
                    Main.log.info("Jar parse:Method "+MRMethodName.ProcessFile+" found");
                    Class<?>[] cc=mm.getParameterTypes();
                    //check amount should be 3
                    if(cc.length!=3) {Main.log.info("Jar parse:Method "+MRMethodName.ProcessFile+" has not exactly 3 parameters "+"(now "+cc.length+")");return false;}
                    //first element 
                    if(!cc[0].equals(java.io.InputStream.class)) {Main.log.info("Jar parse:Method "+MRMethodName.ProcessFile+" has 1 parameter different from "+java.io.InputStream.class.getName());return false;}
                    if(!cc[1].equals(java.lang.String.class)) {Main.log.info("Jar parse:Method "+MRMethodName.ProcessFile+" has 2 parameter different from "+java.lang.String.class.getName());return false;}
                    if(!cc[2].equals(java.lang.String.class)) {Main.log.info("Jar parse:Method "+MRMethodName.ProcessFile+" has 3 parameter different from "+java.lang.String.class.getName());return false;}
                }
                else return false;
                //Check signature of JoinResult Method
                if(mm.getName().equals(MRMethodName.JoinResults.toString())) {
                    Main.log.info("Jar parse:Method "+MRMethodName.JoinResults+" found");
                    Class<?>[] cc=mm.getParameterTypes();
                    if(cc.length!=1) {Main.log.info("Jar parse:Method "+MRMethodName.JoinResults+" should have 1 parameter "+"(now "+cc.length+")");return false;}
                    if(!cc[0].equals(java.lang.String.class)) {Main.log.info("Jar parse:Method "+
                            MRMethodName.JoinResults+" has parameter different from "+java.lang.Object[].class.getName());return false;}                    
                }
                else return false;
            }
            */
            /*
            if (method.equals(MRMethodName.ProcessFile.toString()))
                m = cl.getMethod(method, new Class[]{InputStream.class,String.class,String.class});
            else
                m = cl.getMethod(method, new Class[]{Object[].class}); 
            */
            m=cl.getMethod(method,defReq.getMethodParameters());
            if (!isStdType(m.getReturnType())){
                /*
                Main.log.info("Find Custom type return value "+ m.getReturnType().getCanonicalName());
                //Check that custom type exist in jar
                boolean found=false;
                for(Class<?> allcl:cl.getClasses()){
                    if(allcl.getName().equals(m.getReturnType()))found=true;
                }
                if(!found){Main.log.info("Jar parse:Inner class "+m.getReturnType().getName()+" not found");return false;}
                */
                Class<?> incl = Class.forName (m.getReturnType().getName(), true, loader);
                //for(Class<?> c: cl.getClasses() ){
                                                                           
                //this.innerClass=c.getName().split("\\$")[1];
                this.innerClass=incl.getName().split("\\.")[1];
                //Agent.log.info("Inner Class Name is "+innerClass);
                //Agent.log.info("ReturnType="+m.getReturnType().getName());
                //if(m.getReturnType().equals(c)){
                    //Agent.log.info("ReturnType="+m.getReturnType().getName());
                    for(Field fl: incl.getDeclaredFields()){ 
                        //Agent.log.info("fl="+fl.getName());
                        setTypeProto(fl.getName(),getValueType(fl.getType())); 
                        setTypeJava(fl.getName(),fl.getType().getName());
                    }
                //}
                //standard type of return
                //}
            }
            this.printProto();
        } catch (MalformedURLException ex) {
            Main.log.error(MRHandlerParser.class.getName()+" . MalformedURLException "+ex);
            return false;
        } catch (ClassNotFoundException ex) {
            Main.log.error(MRHandlerParser.class.getName()+" . ClassNotFoundException "+ex);
            return false;
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
            Main.log.error(MRHandlerParser.class.getName()+" . NoSuchMethodException "+ex);
            return false;
        } catch (IOException ex) {
            Logger.getLogger(MRHandlerParser.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
    private void setTypeProto(String name, ProtoType type){
        //reference to superclass
        if (name.equals("this$0")) return;
         this.proto.put(name,type);        
    }
    private void setTypeJava(String name,String type){
        if (name.equals("this$0")) return;
        if (type.startsWith("java.lang.")) type=type.substring(10);
        //this.javaproto.put(type, name); 
        this.javaproto.put(name,type);
    }
    private void printProto() throws IOException{
        Main.log.info("Proto file:");
        for(Map.Entry<String,ProtoType> m:this.proto.entrySet()){
            Main.log.info(m.getKey()+" "+m.getValue());
        }
    }
    private ProtoType getValueType(Class<?> cl){
        String type=cl.getName();
        if (type.startsWith("java.lang.")) type=type.substring(10);
        if (type.startsWith(classname)) return ProtoType.EMPTY;;
        switch (ValueType.valueOf(type.toUpperCase())) {
           case STRING : return ProtoType.STRING;
           case INT: return ProtoType.INT32;
           case OBJECT: return ProtoType.BYTES;
           case DOUBLE: return ProtoType.DOUBLE;
           default: return ProtoType.EMPTY;
        }        
    }       
    private boolean isStdType(Class<?> cl){
        String s=cl.getName();
        if (s.startsWith("java.lang.")) s=s.substring(10);
        //Agent.log.info("innerclass="+s);
        for (ValueType v: ValueType.values()){
            //Agent.log.info("v.name="+v.name()+"s="+s);
            if (v.name().equals(s.toUpperCase())) {
                this.isStdType=true;
                this.stdType=s;
                setTypeProto(s.toLowerCase(),getValueType(cl));
                return true;
            }            
        }
        return false;
    }
    public void writeProto(String protoFileName) throws IOException{        
        PrintWriter pwriter = new PrintWriter(new BufferedWriter(new FileWriter(protoFileName, true)));
        pwriter.println("package "+PACKAGE_NAME+";");
        pwriter.println("option java_package = "+"\""+PACKAGE_NAME+"\""+";");
        pwriter.println("option java_outer_classname = "+"\""+serializatorName+"Proto"+"\""+";");
        pwriter.println("message "+serializatorName+"{");
        int i=1;
        for(Map.Entry<String,ProtoType> m:this.proto.entrySet()){
            pwriter.println("required "+m.getValue().toString().toLowerCase()+" "+m.getKey()+" = "+i+";");
            i++;
        }
        pwriter.println("}");
        pwriter.close();
    }    
    public void invokeProtoc(String protoFileName) throws IOException, InterruptedException{
        //chech that src directory exist
        if(!(new File(currentPath+"src")).exists())
            new File(currentPath+"src").mkdir();
        List<String> cmdProto = new ArrayList<String>();
        cmdProto.add(Main.getProtocPath());
        cmdProto.add("--java_out="+currentPath+File.separator+"src");
        cmdProto.add("--proto_path="+currentPath);
        cmdProto.add(protoFileName);        
        Process pProtoc= new ProcessBuilder(cmdProto).start();
        pProtoc.waitFor();
        pProtoc.destroy();
    }
    public void generateSrc() throws IOException{
        Main.log.info(currentPath+"src"+File.separator+"agent");
        if(!(new File(currentPath+"src"+File.separator+"agent")).exists())
            new File(currentPath+"src"+File.separator+"agent").mkdir();
            //Agent.log.info("Not exist");
        if((new File(currentPath+"src"+File.separator+"agent"+File.separator+"MRSerializator.java")).exists()) {Main.log.info( "Generate Src: File "+
                currentPath+"src"+File.separator+"agent"+File.separator+"MRSerializator.java"+" already exist");
            //delete them
            if(!(new File(currentPath+"src"+File.separator+"agent"+File.separator+"MRSerializator.java")).delete());Main.log.info("Delete file MRSerializator.java");}
        PrintWriter pwriter = new PrintWriter(new BufferedWriter(new FileWriter(currentPath+"src"+File.separator+"agent"+File.separator+"MRSerializator.java", true)));        
        Main.log.info(currentPath+"src"+File.separator+"agent"+File.separator+"MRSerializator.java");
        pwriter.println("package "+PACKAGE_NAME+";");
        pwriter.println("import bigdata.AvgResult.*;");
        //if (stdType!=null)pwriter.println("import bigdata."+this.innerClass+";");
        pwriter.println("import bigdata.AvgResult;");
        pwriter.println("import com.google.protobuf.ByteString;");
        pwriter.println("import java.io.IOException;");
        pwriter.println("import agent."+serializatorName+"Proto."+serializatorName+";");
        if (!this.isStdType){
            pwriter.println("import bigdata."+this.innerClass+";");
            pwriter.println("import bigdata.AvgResult;");        
        }
        else{
            pwriter.println("import java.io.ByteArrayOutputStream;");
            pwriter.println("import java.io.ByteArrayInputStream;;");
            pwriter.println("import java.io.ObjectOutputStream;;");
            pwriter.println("import java.io.ObjectInputStream;");                        
        }
        pwriter.println("public class MRSerializator {");
        //method get serialized to byte[]  
        pwriter.println("public MRSerializator(){}");
        pwriter.println("public "+"byte[] "+"getSerialized(Object tt"+") throws IOException"+"{");
        //pwriter.println("System.out.println(\"start serialize!\");");  
        if(this.isStdType){
            pwriter.println(serializatorName+"Proto."+serializatorName+".Builder mbuilder="+serializatorName+"Proto."+serializatorName+".newBuilder();");
            //need refactor
            if (stdType.equals("Object")){
                pwriter.println("ByteArrayOutputStream b = new ByteArrayOutputStream();");
                pwriter.println("ObjectOutputStream o = new ObjectOutputStream(b);");
                pwriter.println("o.writeObject(tt);");                
                pwriter.println("mbuilder.set"+stdType+"(ByteString.copyFrom(b.toByteArray()));");         
            }
            else{
                if (stdType.equals("int"))
                    pwriter.println("mbuilder.set"+Character.toUpperCase(stdType.charAt(0))+stdType.substring(1)+"((Integer)tt);");                
                else pwriter.println("mbuilder.set"+Character.toUpperCase(stdType.charAt(0))+stdType.substring(1)+"(("+stdType +")tt);");            
            }
                pwriter.println("return mbuilder.build().toByteArray();");
        }
        else {
            pwriter.println(innerClass+" ttt"+"="+"("+innerClass+")"+"tt"+";");
            pwriter.println(serializatorName+"Proto."+serializatorName+".Builder mbuilder="+serializatorName+"Proto."+serializatorName+".newBuilder();");
            for(Map.Entry<String,String> m:this.javaproto.entrySet()){
                pwriter.println("mbuilder.set"+Character.toUpperCase(m.getKey().charAt(0))+m.getKey().substring(1)+"(ttt."+m.getKey()+");");
            }
            pwriter.println("return mbuilder.build().toByteArray();");
        }        
        pwriter.println("}");
        //deserializator source code
        pwriter.println("public "+"Object "+"getDeserialized(byte[] data"+")"+"throws com.google.protobuf.InvalidProtocolBufferException,ClassNotFoundException,IOException"+"{");
        //pwriter.println("System.out.println(\"start deserialize!\");");
        if(isStdType){                  
            if (stdType.equals("Object")){
                pwriter.println(serializatorName+"Proto."+serializatorName+" protobufOb="+serializatorName+"Proto."+serializatorName+".parseFrom(data);");
            pwriter.println("byte [] ob=protobufOb.get"+stdType+"().toByteArray();");          
            pwriter.println("ByteArrayInputStream bis = new ByteArrayInputStream(ob);");
            pwriter.println("ObjectInputStream ois = new ObjectInputStream(bis);");
            
            pwriter.println(stdType+" v = ois.readObject();");
                        pwriter.println("ois.close();");
                        pwriter.println("bis.close();");
            pwriter.println("return v;");            
            }
            else{
            pwriter.println(serializatorName+"Proto."+serializatorName+" protobufOb="+serializatorName+"Proto."+serializatorName+".parseFrom(data);");
            pwriter.println(stdType+" ob=protobufOb.get"+Character.toUpperCase(stdType.charAt(0))+stdType.substring(1)+"();");          
            pwriter.println("return ob;");
            }            
        }
            else {
        //pwriter.println("AvgResult  av=new bigdata.AvgResult();");
        pwriter.println(innerClass+" ob=new "+innerClass+"();");
        pwriter.println(serializatorName+"Proto."+serializatorName+" protobufOb="+serializatorName+"Proto."+serializatorName+".parseFrom(data);");
        for(Map.Entry<String,String> m:this.javaproto.entrySet()){
            pwriter.println("ob."+m.getKey()+"=protobufOb.get"+Character.toUpperCase(m.getKey().charAt(0))+m.getKey().substring(1)+"();");
        }
        pwriter.println("return ob;");
        }        
        pwriter.println("}");        
        pwriter.println("}");
        pwriter.close();
    }
    
    public void generateSerializator() throws IOException, InterruptedException{                   
        List<String> cmd = new ArrayList<String>();
        cmd.add(Main.getJavacPath());
        cmd.add("-cp");
        cmd.add(currentPath+File.separator+"lib/protobuf-java-2.5.0.jar;"+currentPath+File.separator+file);
        //cmd.add("-sourcepath");
        //cmd.add("src/agent/*.java");
        //cmd.add(currentPath+File.separator+"src/agent/MRSerializator.java");
        cmd.add(currentPath+File.separator+"src/agent/*.java");        
        Process pJavac = null;
        //File filepath=null;
        //filepath=new File(currentPath);
        Main.log.info("Cmd Serializator compile: "+cmd.toString());
        ProcessBuilder pbuilder=new ProcessBuilder(cmd);//.directory(new File(currentPath));
        //pbuilder.directory(filepath);        
        pJavac=pbuilder.start();
        //pJavac.directory("/home");        
        pJavac.waitFor();
        if(pJavac.exitValue()!=0)Main.log.error("Compile Serializator:Error while execute command "+cmd.toString());
        else Main.log.info("Success of compile Serializator: "+cmd.toString()+" exit value "+pJavac.exitValue());
        //Thread.sleep(10000);
        //pJavac.destroy();        
        //"c:\\program files\\java\\jdk1.6.0_45\\bin\\jar.exe" cvf ../jar.jar agent/*.class
        List<String> cmdJar = new ArrayList<String>();
        //cmdJar.add("cd "+currentPath+File.separator+"src"+";");
        cmdJar.add(Main.getJarPath());
        cmdJar.add("cf");
        cmdJar.add(currentPath+File.separator+file+"-"+serializatorName+".jar");
        //cmdJar.add("../"+file+"-"+serializatorName+".jar");
        cmdJar.add("-C");
        cmdJar.add(currentPath+File.separator+"src");
        cmdJar.add("agent");
        //cmdJar.add("agent/*.class");
        //File filepath2=new File(currentPath+File.separator+"src");
        Main.log.info("Cmd Serializator build: "+cmdJar.toString());
        ProcessBuilder pJarbldr=(new ProcessBuilder(cmdJar));//.directory(new File(currentPath+File.separator+"src"));
        //pJarbldr.directory(filepath2);
        Process pJar=pJarbldr.start();        
        pJar.waitFor();
        if(pJar.exitValue()!=0)Main.log.error("Build Serializator:Error while execute command "+cmd.toString());
        else Main.log.info("Success of build Serializator: "+cmdJar.toString()+" exit value "+pJar.exitValue());
        //TODO check that del is ok        
        //pJar.destroy();                
        
        String[] fileEntries= (new File(currentPath+File.separator+"src"+File.separator+"agent")).list();
        for(String f:fileEntries)
            (new File(currentPath+File.separator+"src"+File.separator+"agent"+File.separator+f)).delete();
            (new File(currentPath+File.separator+"src"+File.separator+"agent")).delete();    
           
    }    
}
