package itmo.escience.dstorage.agent;

import itmo.escience.dstorage.agent.AgentMethodHandler;
import itmo.escience.dstorage.agent.Ticket;
import itmo.escience.dstorage.agent.requests.AgentRequest;
import itmo.escience.dstorage.agent.requests.AgentRequestType;
import itmo.escience.dstorage.agent.requests.RequestFactory;
import itmo.escience.dstorage.agent.responses.AgentResponse;
import itmo.escience.dstorage.agent.responses.SimpleResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.Locale;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpServerConnection;
import org.apache.http.HttpStatus;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.protocol.HttpService;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

public class AgentHttpService {
    
    public static enum Methods {GET, PUT, POST, DELETE, OPTIONS };
    private static Methods agentMethods;     
    static class DSagentHttpHandler implements HttpRequestHandler  {
        
        private final String docRoot;
        
        public DSagentHttpHandler(final String docRoot) {
            super();
            this.docRoot = docRoot;
        }
        public void handle(
                final HttpRequest request, 
                final HttpResponse response,
                final HttpContext context) throws HttpException, IOException {
            
            String method = request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
            if (!method.equals("GET") && !method.equals("POST") && !method.equals("PUT") && !method.equals("DELETE") && !method.equals("OPTIONS")) {
                throw new MethodNotSupportedException(method + " method not supported"); 
            }           
            String target = request.getRequestLine().getUri();
            agentMethods=Methods.valueOf(method);
            switch(agentMethods) {
                case OPTIONS:
                    response.setHeader("Access-Control-Allow-Origin", "*");
                    response.setHeader("Access-Control-Allow-Methods", "POST, PUT, GET, OPTIONS, DELETE");
                    response.setStatusCode(HttpStatus.SC_OK);
                    StringEntity entity = new StringEntity("{\"Status\":\"OK\"}");
                    entity.setContentType("application/json; charset=UTF-8");
                    response.setEntity(entity);                    
                    break;
                case DELETE:
                    Main.log.info("DELETE Method Detected");
                    try {
                        //AgentRequest requestDelete=RequestFactory.create(request);
                        //AgentResponse responseDelete=requestDelete.process();
                        //responseDelete.convert(response);
                        
                        AgentRequest commandRequest=RequestFactory.create(request);
                        AgentResponse commandResponse=commandRequest.process();
                        ((SimpleResponse)commandResponse).convert(response);
                        
                        //AgentMethodHandler.handleMethodDELETE(request,response);
                    } catch (Exception ex) {
                        Main.log.error("Exception handleMethodDELETE "+ex.getMessage());
                    } 
                    break;
                case GET: 
                    Main.log.info("GET Method Detected");
                    try {    
                        AgentMethodHandler.handleMethodGET(request,response);
                    } catch (Exception ex) {
                        Main.log.error("Exception handleMethodGET "+ex.getMessage());
                        
                    }                                          
                    break;
                case POST:
                    Main.log.info("POST Method Detected");
                case PUT:
                    Main.log.info("PUT Method Detected");    
                    try {                           
                        AgentMethodHandler.handleMethodPUT(request,response);
                    } catch (Exception ex) {
                        Main.log.error("Exception handleMethodPUT "+ex.getMessage());
                        ex.printStackTrace();
                    }  
                    break;
                default: 
                    Main.log.error("Unsupported method");
                    return;
            }
       }    
    
    
    static class DSagentRequestListenerThread extends Thread {

        private final ServerSocket serversocket;
        private final HttpParams params; 
        private final HttpService httpService;
        
        public DSagentRequestListenerThread(int port, final String docroot) throws IOException {
            this.serversocket = new ServerSocket(port);
            this.params = new SyncBasicHttpParams();
            this.params
                .setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 500000)
                .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
                .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
                .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpComponents/1.1");

            // Set up the HTTP protocol processor
            HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpResponseInterceptor[] {
                    new ResponseDate(),
                    new ResponseServer(),
                    new ResponseContent(),
                    new ResponseConnControl()
            });
            
            // Set up request handlers
            HttpRequestHandlerRegistry reqistry = new HttpRequestHandlerRegistry();
            reqistry.register("*", new DSagentHttpHandler(docroot));
            // Set up the HTTP service
            this.httpService = new HttpService(
                    httpproc, 
                    new DefaultConnectionReuseStrategy(), 
                    new DefaultHttpResponseFactory(),
                    reqistry,
                    this.params);
        }
        
        public void run() {
            Main.log.info("Listening on port " + this.serversocket.getLocalPort());
            while (!Thread.interrupted()) {
                try {
                    // Set up HTTP connection
                    Socket socket = this.serversocket.accept();
                    DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
                    Main.log.info("Incoming connection from " + socket.getInetAddress());
                    conn.bind(socket, this.params);

                    // Start worker thread
                    Thread t = new DSagentWorkerThread(this.httpService, conn);
                    t.setDaemon(true);
                    t.start();
                } catch (InterruptedIOException ex) {
                    break;
                } catch (IOException e) {
                    Main.log.error("I/O error initialising connection thread: " 
                            + e.getMessage());
                    break;
                }
            }
        }
    }
    
    static class DSagentWorkerThread extends Thread {

        private final HttpService httpservice;
        private final HttpServerConnection conn;
        
        public DSagentWorkerThread(
                final HttpService httpservice, 
                final HttpServerConnection conn) {
            super();
            this.httpservice = httpservice;
            this.conn = conn;
        }
        
        public void run() {
            Main.log.info("New connection thread");
            HttpContext context = new BasicHttpContext(null);
            try {
                while (!Thread.interrupted() && this.conn.isOpen()) {
                    this.httpservice.handleRequest(this.conn, context);
                }
            } catch (ConnectionClosedException ex) {
                Main.log.info("Client closed connection");
            } catch (IOException ex) {
                Main.log.error("I/O error: " + ex.getMessage());
                ex.printStackTrace();
            } catch (HttpException ex) {
                Main.log.error("Unrecoverable HTTP protocol violation: " + ex.getMessage());
            } finally {
                try {
                    this.conn.shutdown();
                } catch (IOException ignore) {}
            }
        }

    }
    }
}