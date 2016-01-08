package itmo.escience.dstorage.agent;

import java.util.Date;

/**
 *
 * @author anton
 */
public class ReduceLeaderStat extends Stat{
        String taskid;
        ReduceLeaderStat(Date d,String aA,long t, String id){
            this.date=d;
            this.agentAddress=aA;
            this.time=t;
            this.taskid=id;
                                    
        }
    }
