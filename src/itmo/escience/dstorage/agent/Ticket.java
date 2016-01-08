package itmo.escience.dstorage.agent;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.utils.AgentHttpHeaders;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import org.apache.http.HttpRequest;
import org.apache.http.HttpStatus;

public class Ticket {
    public static final String[] securityHeaders={AgentHttpHeaders.Sign.getHeaderString(),AgentHttpHeaders.Ticket.getHeaderString()}; 
    
    public static boolean validateStorageTicket(HttpRequest request) throws Exception  {
        if ((Agent.getConfig().isProperty("Security"))){
            if (!Agent.getConfig().getProperty("Security").equals("2")) return true;//проверка подписи отключена, проверка всегда успешна            
        }
        else return true; //Не задан параметр проверки подписи
        String strTicket,strSign,strUri;
        //Проверка наличия в заголовке запроса полей Ticket и Sign в случае отсутствия выход    
        if (request.containsHeader("Ticket") && request.containsHeader("Sign") ){
            strTicket=request.getFirstHeader("Ticket").getValue();
            strSign=request.getFirstHeader("Sign").getValue();     
        }
        else
        {
            Agent.log.info("Need Ticket and Sign in Http Header, but doesn't have");
            return false;
        }
        //Проверка соответствия Ticket и запроса к агенту       
        Agent.log.info("Sign="+strSign);
        Agent.log.info("Ticket="+strTicket);
        String[] s=strTicket.split(":");
        /*
        for (int i=0;i<s.length;i++)
             Agent.log.info("s["+i+"]="+s[i]);
        Agent.log.info("URL="+URLDecoder.decode(request.getRequestLine().getUri(),Agent.getLocalEncoding()));
        //check method and timestamp && s[1].equals(request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH)
        Agent.log.info("dd="+URLDecoder.decode(request.getRequestLine().getUri(),Agent.getLocalEncoding()));
        Agent.log.info("s[0]="+s[0]);
        */
        strUri=URLDecoder.decode(request.getRequestLine().getUri(),Agent.getLocalEncoding());
        //if 
        if (strUri.startsWith("/"))
            strUri=strUri.substring(1);
        
        //if (!(s.length==6 && s[0].equals(URLDecoder.decode(request.getRequestLine().getUri(),Agent.getLocalEncoding()).substring(1)))
        if (!(s.length==6 && s[0].equals(strUri))
                && s[2].equals(Agent.getAgentAddress())
                && s[3].equals(Agent.getConfig().getProperty("AgentPort"))){
            Agent.log.info("Error verified Ticket");
            return false;
        }
        return verifySign(strTicket, strSign, Agent.getPublicKey());
    }
    public static boolean validateStorageTicket(AgentRequest request) {
        if ((Agent.getConfig().isProperty("Security"))){
            if (!Agent.getConfig().getProperty("Security").equals("2")) return true;//проверка подписи отключена, проверка всегда успешна            
        }
        else return true; //Не задан параметр проверки подписи
        String strTicket,strSign,strUri;
        //Проверка наличия в заголовке запроса полей Ticket и Sign в случае отсутствия выход 
        if (request.checkHeaders(securityHeaders) ){
            strTicket=request.getHeader(AgentHttpHeaders.Ticket.getHeaderString());
            strSign=request.getHeader(AgentHttpHeaders.Sign.getHeaderString());     
        }
        else
        {
            Agent.log.info("Need Ticket and Sign in Http Header, but doesn't have");
            return false;
        }
        //Проверка соответствия Ticket и запроса к агенту       
        /*
        Agent.log.info("Sign="+strSign);
        Agent.log.info("Ticket="+strTicket);
        */
        String[] s=strTicket.split(":");
        
        /*
        for (int i=0;i<s.length;i++)
             Agent.log.info("s["+i+"]="+s[i]);
        */
        try{
            //Agent.log.info("URL="+URLDecoder.decode(request.getTarget(),Agent.getLocalEncoding()));
            //check method and timestamp && s[1].equals(request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH)
            //Agent.log.info("dd="+URLDecoder.decode(request.getTarget(),Agent.getLocalEncoding()));
            //Agent.log.info("s[0]="+s[0]);
            strUri=URLDecoder.decode(request.getTarget(),Agent.getLocalEncoding());
        } catch(UnsupportedEncodingException uee){
            Agent.log.error(Ticket.class.getName()+" "+uee);
            return false;
        }
        //if 
        if (strUri.startsWith("/"))
            strUri=strUri.substring(1);
        
        //if (!(s.length==6 && s[0].equals(URLDecoder.decode(request.getRequestLine().getUri(),Agent.getLocalEncoding()).substring(1)))
        if (!(s.length==6 && s[0].equals(strUri))
                && s[2].equals(Agent.getAgentAddress())
                && s[3].equals(Agent.getConfig().getProperty("AgentPort"))){
            Agent.log.info("Error verified Ticket");
            return false;
        }
        return verifySign(strTicket, strSign, Agent.getPublicKey());
    }
    private static boolean verifySign(String ticket, String signStr, PublicKey publicKey) {
       
        byte[] sign = DatatypeConverter.parseBase64Binary(signStr);
        Signature instance;
        boolean isVerivied=false;
        try {
            instance = Signature.getInstance("SHA1withRSA");
            instance.initVerify(publicKey);
            instance.update(ticket.getBytes("UTF8"));
            isVerivied = instance.verify(sign);
            if (!isVerivied) Agent.log.info("Verified failed");
            //else Agent.log.info("Verified failed");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidKeyException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SignatureException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        }        
        //if (isVerivied) Agent.log.info("Verified ok");
        //else Agent.log.info("Verified failed");
        return isVerivied;
    }
        
}