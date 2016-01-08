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
        if ((Main.getConfig().isProperty("Security"))){
            if (!Main.getConfig().getProperty("Security").equals("2")) return true;//проверка подписи отключена, проверка всегда успешна            
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
            Main.log.info("Need Ticket and Sign in Http Header, but doesn't have");
            return false;
        }
        //Проверка соответствия Ticket и запроса к агенту       
        Main.log.info("Sign="+strSign);
        Main.log.info("Ticket="+strTicket);
        String[] s=strTicket.split(":");
        /*
        for (int i=0;i<s.length;i++)
             Main.log.info("s["+i+"]="+s[i]);
        Main.log.info("URL="+URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        //check method and timestamp && s[1].equals(request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH)
        Main.log.info("dd="+URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()));
        Main.log.info("s[0]="+s[0]);
        */
        strUri=URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding());
        //if 
        if (strUri.startsWith("/"))
            strUri=strUri.substring(1);
        
        //if (!(s.length==6 && s[0].equals(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).substring(1)))
        if (!(s.length==6 && s[0].equals(strUri))
                && s[2].equals(Main.getAgentAddress())
                && s[3].equals(Main.getConfig().getProperty("AgentPort"))){
            Main.log.info("Error verified Ticket");
            return false;
        }
        return verifySign(strTicket, strSign, Main.getPublicKey());
    }
    public static boolean validateStorageTicket(AgentRequest request) {
        if ((Main.getConfig().isProperty("Security"))){
            if (!Main.getConfig().getProperty("Security").equals("2")) return true;//проверка подписи отключена, проверка всегда успешна            
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
            Main.log.info("Need Ticket and Sign in Http Header, but doesn't have");
            return false;
        }
        //Проверка соответствия Ticket и запроса к агенту       
        /*
        Main.log.info("Sign="+strSign);
        Main.log.info("Ticket="+strTicket);
        */
        String[] s=strTicket.split(":");
        
        /*
        for (int i=0;i<s.length;i++)
             Main.log.info("s["+i+"]="+s[i]);
        */
        try{
            //Agent.log.info("URL="+URLDecoder.decode(request.getTarget(),Main.getLocalEncoding()));
            //check method and timestamp && s[1].equals(request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH)
            //Agent.log.info("dd="+URLDecoder.decode(request.getTarget(),Main.getLocalEncoding()));
            //Agent.log.info("s[0]="+s[0]);
            strUri=URLDecoder.decode(request.getTarget(),Main.getLocalEncoding());
        } catch(UnsupportedEncodingException uee){
            Main.log.error(Ticket.class.getName()+" "+uee);
            return false;
        }
        //if 
        if (strUri.startsWith("/"))
            strUri=strUri.substring(1);
        
        //if (!(s.length==6 && s[0].equals(URLDecoder.decode(request.getRequestLine().getUri(),Main.getLocalEncoding()).substring(1)))
        if (!(s.length==6 && s[0].equals(strUri))
                && s[2].equals(Main.getAgentAddress())
                && s[3].equals(Main.getConfig().getProperty("AgentPort"))){
            Main.log.info("Error verified Ticket");
            return false;
        }
        return verifySign(strTicket, strSign, Main.getPublicKey());
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
            if (!isVerivied) Main.log.info("Verified failed");
            //else Main.log.info("Verified failed");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InvalidKeyException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SignatureException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(Ticket.class.getName()).log(Level.SEVERE, null, ex);
        }        
        //if (isVerivied) Main.log.info("Verified ok");
        //else Main.log.info("Verified failed");
        return isVerivied;
    }
        
}