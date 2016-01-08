package itmo.escience.dstorage.agent;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;

public class Perf{
    
    public Perf(){

    }
    public static void IOQueue() throws IOException{
        String st;
        PrintWriter pwriter2=null;
        
        //String exec="typeperf \"\Сведения о процессоре(_Total)";
                //+ "+"\% загруженности процессора\"";      
        
        String exQueue = "typeperf -sc 1 \""+ "\\"+"Физический диск(_Total)"+"\\"+"Текущая длина очереди диска\"";
        try{
            //
            //System.out.println("Ex= "+exQueue);
            pwriter2 = new PrintWriter(new BufferedWriter(new FileWriter("IOQueue_"+Agent.getAgentAddress()+".log", true)));
            Process ps = Runtime.getRuntime().exec(exQueue);
            BufferedReader br= new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while ((st=br.readLine())!=null){
                pwriter2.append(st);
                pwriter2.println();
                }
            pwriter2.close();
        }
            catch (Exception ex){
                System.out.println("Exceptiooo "+ex.toString());
            }
        
    }
    public static void IOSpeed() throws IOException{
        String st;
        PrintWriter pwriter=null;
        //typeperf -sc 1 "\Физический диск(_Total)\Скорость чтения с диска (байт/c)"
        String exSpeed = "typeperf -sc 1 \""+ "\\"+"Физический диск(_Total)"+"\\"+"Скорость чтения с диска (байт/с)\"";
        pwriter = new PrintWriter(new BufferedWriter(new FileWriter("IOSpeed_"+Agent.getAgentAddress()+".log", true)));        
        try{
            //
            Process ps = Runtime.getRuntime().exec(exSpeed);
            BufferedReader br= new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while ((st=br.readLine())!=null){
                pwriter.append(st);
                pwriter.println();
                
            }
            pwriter.close();
        }
            catch (Exception ex){
                System.out.println(ex.toString());
            }
    }
    public static void IOActivity() throws IOException{
        String st;
        PrintWriter pwriter=null;
        //typeperf -sc 1 "\Физический диск(_Total)\% активность диска"
        //typeperf "\Физический диск(_Total)\% активности диска при чтении"
        String exAct = "typeperf -sc 1 \""+ "\\"+"Физический диск(_Total)"+"\\"+"% активности диска при чтении\"";
        pwriter = new PrintWriter(new BufferedWriter(new FileWriter("IOAct_"+Agent.getAgentAddress()+".log", true)));        
        try{
            //
            Process ps = Runtime.getRuntime().exec(exAct);
            BufferedReader br= new BufferedReader(new InputStreamReader(ps.getInputStream()));
            while ((st=br.readLine())!=null){
                pwriter.append(st);   
                pwriter.println();

            }
            pwriter.close();
        }
            catch (Exception ex){
                System.out.println(ex.toString());
            }
    }
        public static String showMemory () {
       
        StringBuilder sb = new StringBuilder();

        // Total amount of free memory available to the JVM
        long freeMem = Runtime.getRuntime().freeMemory();
        String freeMemHuman = humanQoutaSize(freeMem);
        sb.append("Total amount of free memory available to the JVM:     ").append(freeMemHuman).append("<br>");

        // Total memory currently in use by the JVM 
        long totalMem = Runtime.getRuntime().totalMemory();
        String totalMemHuman = humanQoutaSize(totalMem);
        sb.append("Total amount memory currently in use by the JVM:      ").append(totalMemHuman).append("<br>");
        
        // Maximum amount of memory the JVM will attempt to use 
        long maxMem = Runtime.getRuntime().maxMemory();
        String maxMemHuman = humanQoutaSize(maxMem);
        sb.append("Maximum amount of memory the JVM will attempt to use: ").append(maxMemHuman).append("<br>");

        return sb.toString();
    }
        public static String  humanQoutaSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        //return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + "\t" + units[digitGroups];
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }
}


