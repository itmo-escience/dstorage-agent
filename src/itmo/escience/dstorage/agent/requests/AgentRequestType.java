package itmo.escience.dstorage.agent.requests;

/**
 *
 * @author Anton Spivak
 */
public enum AgentRequestType {
        UPLOADFILE(""),
        DELETEFILE(""),
        CMD(""),
        ZIP(""),
        DOWNLOADFILE("");
        private final String uri;
        AgentRequestType(String uri){this.uri=uri;}
        public final String getUri(){return this.uri;}
    }
