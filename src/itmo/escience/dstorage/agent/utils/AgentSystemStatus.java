package itmo.escience.dstorage.agent.utils;

/**
 *
 * @author anton
 */
public enum AgentSystemStatus 
    {
    OK("OK"),
    FAILED("Error");    
    private final String mes;
    AgentSystemStatus(String m){this.mes=m;}
    public final String getString(){return this.mes;}
}
    

