package itmo.escience.dstorage.agent.requests;
import itmo.escience.dstorage.agent.handlers.UploadFileHandler;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.utils.AgentHttpHeaders;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequest;

/**
 *
 * @author anton
 */
public class UploadFileRequest extends AgentRequest{
    private final static AgentRequestType type=AgentRequestType.UPLOADFILE; 
    private final StorageLevel storageLevel;
    private long cmdid=-1;
    private HttpEntity entity;
    public HttpEntity getEntity(){return entity;}
    public long getCmdid(){return cmdid;}
    public UploadFileRequest(HttpRequest request){
        super(request);
        entity=((HttpEntityEnclosingRequest)request).getEntity(); 
        int istorage=0;
        if(checkHeaders(new String[]{AgentHttpHeaders.CmdID.getHeaderString()})) cmdid=Long.parseLong(getHeader(AgentHttpHeaders.CmdID.getHeaderString()));
        if(!checkHeaders(new String[]{AgentHttpHeaders.StorageLevel.getHeaderString()})) istorage=-1;//storageType header not in request. Use default value
        else {
            istorage=Integer.parseInt(getHeader(AgentHttpHeaders.StorageLevel.getHeaderString()));
        }
        switch(istorage){
            case 0: storageLevel=StorageLevel.HDD;break;
            case 1: storageLevel=StorageLevel.SSD;break;
            case 2: storageLevel=StorageLevel.MEM;break;
            case -1: storageLevel=StorageLevel.NOTSET;break;
            default: storageLevel=StorageLevel.HDD;break;
        }        
    }    
    @Override
    public AgentResponse process() {
        UploadFileHandler handler= new UploadFileHandler();
        return handler.handle(this);
    }

    @Override
    public AgentRequestType getType() {return type;}
    public StorageLevel getStorageType(){return storageLevel;}    
}
