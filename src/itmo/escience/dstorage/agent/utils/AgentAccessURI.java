package itmo.escience.dstorage.agent.utils;

/**
 *
 * @author Anton Spivak
 */
public enum AgentAccessURI {
    REMOTERESOURCE("/remoteresource",""),
    EMPTY("",""),
    MAP("/map","POST"),
    ZIP("/zip","POST"),
    REDUCE("/reduce","POST"),
    AGENTGET("/command","GET"),
    LVLMGMNT("/command","GET"),
    AGENTSTAT("/stat","GET"),
    AGENTSTATUS("/status","GET");
    private final String mes;
    private final String method;
    AgentAccessURI(String m,String method){this.mes=m;this.method=method;}
    public final String getString(){return this.mes;}
    public static AgentAccessURI parseUri(String uri){
        if(uri.toUpperCase().startsWith("/STATUS")){
            return AgentAccessURI.AGENTSTATUS;
        }
        if(uri.toUpperCase().startsWith("/COMMAND")){
            String[] p=uri.split("[/]");
            String[] params=p[2].split("&");
            for(String s:params){
                String[] tmp=s.split("=");
                if(tmp[0].equals("cmd")&& (tmp[1].equals("move")||tmp[1].equals("copy"))) return AgentAccessURI.LVLMGMNT;
                if(tmp[0].equals("cmd")&& tmp[1].equals("get")) return AgentAccessURI.AGENTGET;
            }               
            return AgentAccessURI.AGENTSTATUS;
        }
        if(uri.toUpperCase().startsWith("/REMOTERESOURCE")){
            return AgentAccessURI.REMOTERESOURCE;
        }
        if(uri.toUpperCase().startsWith("/MAP")){
            return AgentAccessURI.MAP;
        }
        if(uri.toUpperCase().startsWith("/REDUCE")){
            return AgentAccessURI.MAP;
        }
        if(uri.toUpperCase().startsWith("/ZIP")){
            return AgentAccessURI.ZIP;
        }
        if(uri.toUpperCase().startsWith("/STAT")){
            return AgentAccessURI.AGENTSTAT;
        }
        return AgentAccessURI.EMPTY;
    }
}
