package itmo.escience.dstorage.agent.utils;

/**
 *
 * @author anton
 */
public enum AgentMessage {
        OK("{\"Status\":\"OK\"}"),
        FORBIDDEN("{\"Status\":\"Access denied\"}"),
        FAILED("The operation is failed"),
        NOTFOUND("The object not found");
        private final String mes;
        AgentMessage(String m){this.mes=m;}
        public final String getString(){return this.mes;}
        public final String getJSON(){return this.mes;}
        
    }
