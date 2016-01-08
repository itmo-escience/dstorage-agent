package itmo.escience.dstorage.agent.requests;

import itmo.escience.dstorage.agent.handlers.DownloadFileHandler;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.utils.AgentHttpHeaders;
import itmo.escience.dstorage.agent.utils.StorageLevel;
import org.apache.http.HttpRequest;

/**
 *
 * @author anton
 */
public class DownloadFileRequest extends AgentRequest {
    
    private final static AgentRequestType type=AgentRequestType.DOWNLOADFILE; 
    private final StorageLevel storageLevel;
    
    public DownloadFileRequest(HttpRequest httpRequest){
        super(httpRequest);
        int istorage;
        if(!checkHeaders(new String[]{AgentHttpHeaders.StorageLevel.getHeaderString()})) istorage=-1;//storageType header not in request. Use NOTSET value
        else {
            istorage=Integer.parseInt(getHeader(AgentHttpHeaders.StorageLevel.getHeaderString()));
        }
        switch(istorage){
            case 0: storageLevel=StorageLevel.HDD;break;
            case 1: storageLevel=StorageLevel.SSD;break;
            case 2: storageLevel=StorageLevel.MEM;break;
            default: storageLevel=StorageLevel.NOTSET;break;
        }
    }
    @Override
    public AgentRequestType getType() {return type;}

    @Override
    public AgentResponse process() {
        DownloadFileHandler handler=new DownloadFileHandler();
        return handler.handle(this);
    }
    public StorageLevel getStorageLevel(){return storageLevel;}    
}
