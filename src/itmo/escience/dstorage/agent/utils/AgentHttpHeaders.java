package itmo.escience.dstorage.agent.utils;

/**
 *
 * @author anton
 */
public enum AgentHttpHeaders {
    Sign("Sign"),
    Ticket("Ticket"),
    StorageLevel("StorageLevel"),
    CmdID("CmdID");
    private final String str;
    AgentHttpHeaders(String m){this.str=m;}
    public final String getHeaderString(){return this.str;}
}
