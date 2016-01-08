package itmo.escience.dstorage.agent;

import java.io.InputStream;

/**
 *
 * @author Anton Spivak
 */
public enum DefinitionAvgResult {
    ProcessFile(InputStream.class,String.class,String.class),
    JoinResults(Object[].class);    
    private final Class<?>[] params; 
    private static final String jarClassName="bigdata.AvgResult";
    public enum ValueType {STRING, INT, BOOLEAN,DOUBLE,FLOAT,EMPTY,BYTESTRING,OBJECT };
    //private enum stdType{java.lang.};
    DefinitionAvgResult(Class<?>... pr){
        this.params=pr;
    }
    public Class<?>[] getMethodParameters() throws Exception{
        return this.params;
    }
    public String getStrMethodParameters(){
        StringBuilder cb=new StringBuilder();
        for(Class<?> c:params)
            cb.append(c.getCanonicalName());
        return cb.toString();
    }
    public String getMethodName(){
        return this.name();
    }
    public String getJarClassName(){
        return jarClassName;
    }
    public boolean isStdType(Class<?> type){
        //type.getCanonicalName();
        
        //return ValueType.
        for(ValueType s:DefinitionAvgResult.ValueType.values()){
            //Agent.log.info(s.toString()+" "+type.getSimpleName().toUpperCase());
            if(s.toString().equals(type.getSimpleName().toUpperCase())) return true;
        }
        return false; 
    }
}
