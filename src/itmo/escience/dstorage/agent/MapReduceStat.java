package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.MRTimeStat.TimeMarker;
import itmo.escience.dstorage.agent.utils.HttpConn;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.entity.InputStreamEntity;
import org.json.simple.JSONObject;

/**
 *
 * @author anton
 */
public class MapReduceStat {
    Queue <MapStat> mapstat;
    Queue <ReduceStat> reducestat;
    Queue <MapTotalStat> maptotalstat;
    Queue <ReduceLeaderStat> reduceldstat;
    Queue <MRTimeStat> mrtimestat;
    
    //Map <String,MRTimeStat> m;
    private final static int QUEUE_MAPSTAT_SIZE=2000;
    private final static int QUEUE_REDUCESTAT_SIZE=20;
    
    public MapReduceStat(){
        //init all stat structure
        mapstat=new LinkedBlockingQueue<MapStat> ();
        reducestat=new LinkedBlockingQueue<ReduceStat> ();
        maptotalstat=new LinkedBlockingQueue<MapTotalStat> ();
        reduceldstat=new LinkedBlockingQueue<ReduceLeaderStat> ();
        mrtimestat=new LinkedBlockingQueue<MRTimeStat> ();        
    }
    public void addStat(MapStat m){
        
        this.mapstat.add(m);
        //check size of queue
        if (mapstat.size()>QUEUE_MAPSTAT_SIZE){
            mapstat.poll();
        }
    }
    public void addStat(MapTotalStat mtotal){
        this.maptotalstat.add(mtotal);
        if (maptotalstat.size()>QUEUE_MAPSTAT_SIZE){
            maptotalstat.poll();
        }
    }
    public void addStat(ReduceStat reducetst){
        this.reducestat.add(reducetst);
        if (reducestat.size()>QUEUE_REDUCESTAT_SIZE){
            reducestat.poll();
        }
    }
    public void addStat(ReduceLeaderStat r){
        this.reduceldstat.add(r);
        if (reduceldstat.size()>QUEUE_REDUCESTAT_SIZE){
            reduceldstat.poll();
        }
    }
    public void addMrTimeStat(String id, long time, TimeMarker num ){
        
        Iterator<MRTimeStat> itermap= this.mrtimestat.iterator();
        while(itermap.hasNext()){
            MRTimeStat mr=itermap.next();
            if (mr.id.equals(id)){
                mr.setTimeMarker(time, num);
                //break;
                return;
            }
        }
         
                MRTimeStat mrn=new MRTimeStat();
                mrn.id=id;
                mrn.setTimeMarker(time, num);
                //mrn.
                this.mrtimestat.add(mrn);
            
            /*
            if (!ms.isWrited){
                writeStat("MapLog_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                "map "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(ms.date))+" "+ms.agentAddress+" "+ms.session+" "+ms.fileName+" "+
                    Long.toString(ms.time));
                ms.isWrited=true;
            }
            */
        //check that allowed size of stat queue not exceeded        
        //if (mrtimestat.size()>QUEUE_REDUCESTAT_SIZE){
        //    mrtimestat.poll();
        //}
    }
    
    public void getStat(){
        
    }
    public void writeStatToFiles(){
        //mapstats
        //System.out.println("map.size="+Main.getMapReduceStat().mapstat.size());
        //System.out.println("maptotal.size="+Main.getMapReduceStat().maptotalstat.size());
        
        //mapStat
        Iterator<MapStat> itermap= this.mapstat.iterator();
        while(itermap.hasNext()){
            MapStat ms=itermap.next();
            if (!ms.isWrited){
                writeStat("MapLog_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                "map "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(ms.date))+" "+ms.agentAddress+" "+ms.session+" "+ms.fileName+" "+
                    Long.toString(ms.time));
                ms.isWrited=true;
            }
        }
        //map Total Stat
        Iterator<MapTotalStat> itermaptl= this.maptotalstat.iterator();
        while(itermaptl.hasNext()){
            MapTotalStat ts=itermaptl.next();
            if (!ts.isWrited){
            writeStat("MapLogAv_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                "map av"+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(ts.date))+" "+ts.agentAddress+" "+ts.taskid+" "+
                    Long.toString(ts.time));
                ts.isWrited=true;
            }
        }
        //reduce stat
        Iterator<ReduceStat> iterrd= this.reducestat.iterator();
        while(iterrd.hasNext()){
            ReduceStat tsr=iterrd.next();
            if (!tsr.isWrited){
            writeStat("MapLogReduce_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                "reduce "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(tsr.date))+" "+tsr.agentAddress+" "+tsr.taskid+" "+
                    Long.toString(tsr.time));
                tsr.isWrited=true;
            }
        }
        //reduce leader stat
        Iterator<ReduceLeaderStat> iterrdld= this.reduceldstat.iterator();
        while(iterrdld.hasNext()){
            ReduceLeaderStat tsrl=iterrdld.next();
            if (!tsrl.isWrited){
                writeStat("MapLogLeader_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                "reduce leader "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(tsrl.date))+" "+tsrl.agentAddress+" "+tsrl.taskid+" "+
                    Long.toString(tsrl.time));
                tsrl.isWrited=true;
            }
        }
        //mrtimestat
        //Iterator<MRTimeStat> mrs_iter= this.mrtimestat.iterator();
        //while(mrs_iter.hasNext()){
           // MRTimeStat mrstat=mrs_iter.next();
            //if (!mrstat.isWrited){
               // writeStat("MapLogLeader_"+Main.getAgentAddress()+"-"+(new SimpleDateFormat("yyyy-MM-dd").format(new Date()))+".log",
                //"reduce leader "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(tsrl.date))+" "+tsrl.agentAddress+" "+tsrl.taskid+" "+
                //    Long.toString(tsrl.time));
              //  tsrl.isWrited=true;
            //}
        //}
    }
    public void writeStat(String logFile, String line){            
        PrintWriter pwriter=null;
        try {
            pwriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
        } catch (FileNotFoundException ex) {
            Main.log.error(ProcessFile.class.getName()+" . Error create log file for MapReduce "+ex);
        } catch (IOException ex) {
            Main.log.error(ProcessFile.class.getName()+" . IO Error while create log file for MapReduce "+ex);
        }
        pwriter.append(line);
        
        //pwriter.append("map "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new Date()))+" "+Main.getAgentAddress()+" "+session+" "+fileName+" "+
        //            Long.toString(time);
        pwriter.println();
        pwriter.close();
            
    }
    public String getStatHtml(){
        String html="";
        Iterator<MapStat> msiter= this.mapstat.iterator();
        html="<html>Map Statistics per file <br>";
            
        while(msiter.hasNext()){
            MapStat ms=msiter.next();
           
            html=html.concat("map "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(ms.date))+" "+ms.agentAddress+" "+ms.session+" "+ms.fileName+" "+
                    Long.toString(ms.time));
            html=html.concat("<br>");
            
        }        
        //Map Total
        html=html.concat("Map Statistics Total per task<br>");   
        Iterator<MapTotalStat> mst_iter= this.maptotalstat.iterator();
        while(mst_iter.hasNext()){
            MapTotalStat mst=mst_iter.next();
           
            html=html.concat("map av "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(mst.date))+" "+mst.agentAddress+" "+mst.taskid+" "+
                    Long.toString(mst.time));
            html=html.concat("<br>");
            
        }
        //Reduce statistics per task
        html=html.concat("Reduce Statistics per task<br>");   
        Iterator<ReduceStat> rd_iter= this.reducestat.iterator();
        while(rd_iter.hasNext()){
            ReduceStat rds=rd_iter.next();
           
            html=html.concat("reduce "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(rds.date))+" "+rds.agentAddress+" "+rds.taskid+" "+
                    Long.toString(rds.time));
            html=html.concat("<br>");
            
        }
        //Leader statistics per task
        html=html.concat("Leader Statistics per task<br>");   
        Iterator<ReduceLeaderStat> rdl_iter= this.reduceldstat.iterator();
        while(rdl_iter.hasNext()){
            ReduceLeaderStat rdsl=rdl_iter.next();
           
            html=html.concat("reduce leader "+(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(rdsl.date))+" "+rdsl.agentAddress+" "+rdsl.taskid+" "+
                    Long.toString(rdsl.time));
            html=html.concat("<br>");
            
        }
        html=html.concat("Task Time Markers<br>");   
        Iterator<MRTimeStat> mr_iter= this.mrtimestat.iterator();
        while(mr_iter.hasNext()){
            MRTimeStat mrs=mr_iter.next();
           
            html=html.concat("mr time markers "+" "+mrs.id+" "+
                    Long.toString(mrs.t3)+" "+Long.toString(mrs.t4)+" "+Long.toString(mrs.t5)+" "+Long.toString(mrs.t6)+" "+Long.toString(mrs.t7)+" "+Long.toString(mrs.t8));
            html=html.concat("<br>");
            
        }
        
        html=html.concat("</html>");
        return html;
    }
    //send MrStat to another host by POST with json
    public void sendMrTimeStat(String host, String port, String id){       
        JSONObject json=new JSONObject();
        //search mrtimestat with indicated id, get them in json and delete from queue
        Iterator<MRTimeStat> itermap= this.mrtimestat.iterator();
        while(itermap.hasNext()){
            MRTimeStat mr=itermap.next();
            if (mr.id.equals(id)){
                json=mr.toJSON();
                
                mrtimestat.poll();
            }
        }
        Main.log.info("mrtimestat="+json.toString());
        try{
            HttpConn httpconn = new HttpConn();
            httpconn.setup(host,port); 
            httpconn.setMethod("PUT","/mrtimestat");
            httpconn.setEntity(json);
            httpconn.connect();
            httpconn.close();
        } catch (UnknownHostException ex) {
            Main.log.error("MapReduceStat "+ex.getLocalizedMessage());
        } catch (Exception ex) {
            Logger.getLogger(MapReduceStat.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    /*
    class Stat {
        Date date;
        String agentAddress;        
        long time;
    }
    public class MapStat extends Stat {
        String session;
        String fileName;
        MapStat(Date d,String aA,long t, String se, String fn){
            this.date=d;
            this.agentAddress=aA;
            this.time=t;
            this.fileName=fn;
            this.session=se;                        
        }
    }
    */
    //class MapTotalStat extends Stat {
    //    String taskid;
    //}
    //class ReduceStat extends Stat{
    //    String taskid;
    //}
    //class ReduceLeaderStat extends Stat{
    //    String taskid;
    //}
    class LeaderStat{
        
    }
}
