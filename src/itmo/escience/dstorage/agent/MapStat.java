package itmo.escience.dstorage.agent;

import java.util.Date;

/**
 *
 * @author anton
 */
public class MapStat extends Stat {
        String session;
        String fileName;
        MapStat(Date d,String aA,String se, String fn,long t){
            this.date=d;
            this.agentAddress=aA;
            this.time=t;
            this.fileName=fn;
            this.session=se;                        
        }
    }
