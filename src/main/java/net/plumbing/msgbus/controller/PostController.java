package net.plumbing.msgbus.controller;
import com.google.common.io.CharStreams;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplateVO;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import net.plumbing.msgbus.common.json.XML;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;

import javax.servlet.ServletRequest;
import java.io.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;

import static net.plumbing.msgbus.common.XMLchars.*;

@Controller
public class PostController {

    public static final Logger Controller_log = LoggerFactory.getLogger(PostController.class);
    @PostMapping(path = "/HermesService/PostHttpRequest/*", produces = MediaType.ALL_VALUE,  consumes = MediaType.ALL_VALUE)
 //   @ResponseStatus(HttpStatus.OK) // MediaType.TEXT_XML_VALUE.APPLICATION_XML_VALUE

    public @ResponseBody byte[] PostHttpRequest( ServletRequest postServletRequest, HttpServletResponse postResponse) {

        InputStream inputStream=null;
        String Response="ok";
        Long timeOutInMilliSec = 100000L;
        String timeOutResp = "Time Out.";
        Charset  charSet=null;
        HttpServletRequest httpRequest=(HttpServletRequest)postServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String soapAction = httpRequest.getHeader( "soapAction" );
        Controller_log.info("PostHttpRequest: url= (" + url + ") soapAction:"  + soapAction);

        String Url_Soap_Send = findUrl_Soap_Send(url);

        Integer Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);

        if ( Interface_id < 0) {
            postResponse.setStatus(500);
            String OutResponse =Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                    httpRequest.getMethod() + ": Интерфейс для обработки (" + Url_Soap_Send + ") в системе не сконфигурирован" +
                    Fault_End + Body_End + Envelope_End;
            return OutResponse.getBytes();
        }


        try  {
             inputStream = postServletRequest.getInputStream();
        }
        catch ( IOException ioException) {
            postResponse.setStatus(500);
            ioException.printStackTrace(System.err);
            String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                    "postServletRequest.getInputStream fault:" + ioException.getMessage() +
                    Fault_End + Body_End + Envelope_End;
            return OutResponse.getBytes();
        }
        MessageDetails Message = new MessageDetails();
        // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
        int MessageTemplateVOkey= MessageRepositoryHelper.look4MessageTemplate_2_Interface( Interface_id, Controller_log);
        String PropEncoding_Out="UTF-8";
        String PropEncoding_In="UTF-8";
        boolean isDebugged= false;


        if ( MessageTemplateVOkey >= 0 ) {
            String ConfigExecute = MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getConfigExecute();
            Controller_log.info("ConfigExecute:" + ConfigExecute );
            if ( ConfigExecute != null) {
                Properties properties = new Properties();
                InputStream propertiesStream = new ByteArrayInputStream( ConfigExecute.getBytes(StandardCharsets.UTF_8));
                try {
                    properties.load(propertiesStream);
                    for (String key : properties.stringPropertyNames()) {
                        if ( key.equals(MessageTemplateVO.PropNameCharOut)) PropEncoding_Out = properties.getProperty(key);
                        Charsets.toCharset(PropEncoding_Out);
                        if ( key.equals(MessageTemplateVO.PropNameCharIn)) PropEncoding_In = properties.getProperty(key);
                        Controller_log.info( "Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                        if ( key.equals(MessageTemplateVO.PropDebug) ) {
                            Controller_log.info("PropDebug Property[" + key +"]=[" + properties.getProperty(key) + "]" );
                            if (( properties.getProperty(key).equalsIgnoreCase("on") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("full") )
                            )
                            {
                                isDebugged=true;
                            }
                            if (( properties.getProperty(key).equalsIgnoreCase("ON") ) ||
                                    ( properties.getProperty(key).equalsIgnoreCase("FULL") )
                            )
                            {
                                isDebugged=true;
                            }
                        }
                    }
                }catch ( IOException ioException) {
                    postResponse.setStatus(500);
                    ioException.printStackTrace(System.err);
                    Controller_log.error( "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage());
                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                            "properties.load('" + ConfigExecute + "') fault:" + ioException.getMessage() +
                            Fault_End + Body_End + Envelope_End;
                    return OutResponse.getBytes();
                }
            }
        }
// InputStreamReader(inputStream, "Windows-1251"));

        try( InputStreamReader reader = new InputStreamReader(inputStream, Charsets.toCharset(PropEncoding_In))// Charsets.UTF_8)
           )
        {   if ( soapAction != null)
                 Message.XML_MsgInput = CharStreams.toString(reader);
            else {
                int xmlVersionEncoding_pos = 1;
                Message.XML_MsgConfirmation.append(CharStreams.toString(reader)) ;
                if ( Message.XML_MsgConfirmation.substring(1,2).equals("<?") )  {
                    // в запросе <?xml version="1.0" encoding="UTF-8"?> Ищем '?>' что бы изъять !
                    xmlVersionEncoding_pos = Message.XML_MsgConfirmation.indexOf("?>") + 2;
                }
                Message.XML_MsgInput = Envelope_noNS_Begin
                        + Header_noNS_Begin + Header_noNS_End
                        + Body_noNS_Begin
                        + Message.XML_MsgConfirmation.substring(xmlVersionEncoding_pos)// CharStreams.toString(reader)
                        + Body_noNS_End + Envelope_noNS_End
                ;

            }
            inputStream.close();
        }catch ( IOException ioException) {
            postResponse.setStatus(500);
            Controller_log.error( "CharStreams.toString(getInputStream) fault:" + ioException.getMessage());
            if ( soapAction != null) {
                String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                        "CharStreams.toString(getInputStream) fault:" +ioException.getMessage() +
                        Fault_End + Body_End + Envelope_End;
                return OutResponse.getBytes();
            }
            else {
                String OutResponse = Fault_Server_noNS_Begin + "CharStreams.toString(getInputStream) fault:" + ioException.getMessage()
                        + Fault_noNS_End;
                return OutResponse.getBytes();
            }
        }
          // очищаем использованный XML_MsgConfirmation
           Message.XML_MsgConfirmation.setLength(0); Message.XML_MsgConfirmation.trimToSize();


        MessageReceiveTask messageReceiveTask = new MessageReceiveTask( );// (MessageSendTask) context.getBean("MessageSendTask");

        Long Queue_ID = messageReceiveTask.ProcessInputMessage( Interface_id, Message, MessageTemplateVOkey, isDebugged );

        // Controller_log.info("SOAP_1_1_CONTENT_TYPE=" + SOAP_1_1_CONTENT_TYPE );

        if ( Queue_ID == 0L){
            postResponse.setStatus(200);
            if ( soapAction != null) // это SOAP
            { postResponse.setContentType("text/xml; charset=utf-8");
                String OutResponse = Envelope_Begin + Empty_Header + Body_Begin +
                        Message.XML_MsgResponse.toString() +
                        Body_End + Envelope_End;
                if ( isDebugged )
                messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                try {
                    if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                        messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                    messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                } catch (SQLException SQLe) {
                    Controller_log.error(SQLe.getMessage());
                    Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                    SQLe.printStackTrace();
                }
                return OutResponse.getBytes();
            }
            else
            {  // это ЛИРА :(
                postResponse.setContentType("text/xml; charset=" + PropEncoding_Out );
                try {
                    // очищаем использованный XML_MsgConfirmation
                    Message.XML_MsgConfirmation.setLength(0); Message.XML_MsgConfirmation.trimToSize();
                    byte[] OutResponse = Message.XML_MsgResponse.toString().getBytes(PropEncoding_Out);
                    Message.XML_MsgConfirmation.append( new String(OutResponse));
                    if ( isDebugged )
                    Controller_log.warn( "XML_MsgResponse Encoding  (" + PropEncoding_Out + "):" + Message.XML_MsgConfirmation);
                    if ( isDebugged )
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, Message.XML_MsgResponse.toString(), Controller_log  );
                    try {
                        if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                    }
                    return OutResponse;
                    // Message.XML_MsgConfirmation.append(Message.XML_MsgResponse.toString().getBytes(PropEncoding_Out) );
                } catch (UnsupportedEncodingException e) {
                    System.err.println("[ XML_MsgResponse Encoding:" + PropEncoding_Out + "] UnsupportedEncodingException");
                    e.printStackTrace();
                    Controller_log.error( "XML_MsgResponse Encoding fault (" + PropEncoding_Out + ") UnsupportedEncodingException:" + e.getMessage());
                    String OutResponse =  "<DATA><FIELDS><ER>20</ER><MES>" + "XML_MsgResponse Encoding fault (" + PropEncoding_Out + ") UnsupportedEncodingException:" + e.getMessage() +"</MES></FIELDS></DATA>" ;
                    if ( isDebugged )
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                    try {
                        if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                    }
                    return OutResponse.getBytes();
                }

            }
        }
        else {
            postResponse.setStatus(500);
            if ( soapAction != null) // это SOAP
            {
                if ( Queue_ID > 0L ) {
                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Client_Begin +
                            XML.escape(Message.MsgReason.toString() ) +
                            Fault_End + Body_End + Envelope_End;
                    if ( isDebugged )
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                    if ( isDebugged )
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                    try {
                        if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                            messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                        messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                    } catch (SQLException SQLe) {
                        Controller_log.error(SQLe.getMessage());
                        Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                        SQLe.printStackTrace();
                    }
                    return OutResponse.getBytes();

                } else {
                    String OutResponse = Envelope_Begin + Empty_Header + Body_Begin + Fault_Server_Begin +
                            XML.escape( Message.MsgReason.toString()) +
                            Fault_End + Body_End + Envelope_End;
                    if ( isDebugged )
                        messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                    return OutResponse.getBytes();
                }
            }
            else {  // это ЛИРА :(
                String OutResponse =  "<DATA><FIELDS><ER>500</ER><MES>" + XML.escape( Message.MsgReason.toString()) + "</MES></FIELDS></DATA>" ;
                if ( isDebugged )
                    messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                if ( isDebugged )
                    messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, OutResponse, Controller_log  );
                try {
                    if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                        messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                    messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                } catch (SQLException SQLe) {
                    Controller_log.error(SQLe.getMessage());
                    Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                    SQLe.printStackTrace();
                }
                return OutResponse.getBytes();
            }
        }

    }
    /*
    public String // DeferredResult<String>
    PostHttpRequest(@RequestBody ServletRequest postServletRequest, HttpServletResponse postResponse)
    {
        //@PathVariable
        HttpServletRequest httpRequest=(HttpServletRequest)postServletRequest;
        Long timeOutInMilliSec = 100000L;
        String timeOutResp = "Time Out.";
        Charset  charSet=null;
        //DeferredResult<String> deferredResult = new DeferredResult<>(timeOutInMilliSec,timeOutResp);

        String url = httpRequest.getRequestURL().toString();
        String encoding = httpRequest.getCharacterEncoding();
        if(encoding!=null)
        { charSet= Charset.forName(encoding);
            if(charSet!=null) encoding=charSet.name();
            else  { System.out.println("Unkhown charaEncoding="+encoding); encoding=null; }
        }

        //postResponse.setContentType("text/xml;charset=UTF-8");
        //postResponse.addHeader("Access-Control-Allow-Origin","*");
        String data = "getRemoteAddr:" + httpRequest.getRemoteAddr() + " isAsyncSupported:" + httpRequest.isAsyncSupported();
        String queryString = httpRequest.getQueryString();
        Controller_log.info("PostHttpRequest: url= (" + url + ") queryString(" + data + ")");
        Controller_log.info("PostHttpRequest:  httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" );
        String Url_Soap_Send = findUrl_Soap_Send(url);

        Integer Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        if ( Interface_id < 0)
            return  "404\n"+ " getServletPath" + httpRequest.getServletPath();

        deferredResult.setResult( "404\n"+ " getServletPath" + httpRequest.getServletPath());

        deferredResult.setResult(
                Envelope_Begin + Empty_Header + Body_Begin + Fault_Begin +
                        httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" +
                        Fault_End + Body_End + Envelope_End
        );

        return XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" );

                Envelope_Begin + Empty_Header + Body_Begin + Fault_Begin +
                httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" +
                Fault_End + Body_End + Envelope_End;

    }
*/
}
