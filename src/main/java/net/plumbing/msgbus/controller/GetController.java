package net.plumbing.msgbus.controller;

import net.plumbing.msgbus.common.json.JSONException;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import net.plumbing.msgbus.common.json.JSONObject;
import net.plumbing.msgbus.common.json.XML;

import javax.servlet.ServletRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;

import static net.plumbing.msgbus.common.XMLchars.*;

@RestController
//@RequestMapping("/HermesService")
public class GetController {

    private static final Logger Controller_log = LoggerFactory.getLogger(GetController.class);

    @GetMapping(path ="/HermesService/GetHttpRequest/*", produces = MediaType.APPLICATION_XML_VALUE,  consumes = MediaType.ALL_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody

    public String GetHttpRequest( ServletRequest getServletRequest, HttpServletResponse getResponse) {
        //@PathVariable
        HttpServletRequest httpRequest = (HttpServletRequest) getServletRequest;

        String url = httpRequest.getRequestURL().toString();
        String queryString;
        try {
            queryString = URLDecoder.decode(httpRequest.getQueryString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            queryString = httpRequest.getQueryString();
        }
        Controller_log.warn("url= (" + url + ") queryString(" + queryString + ")");
        Controller_log.warn("httpRequest.getMethod()" + httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")");
        String Url_Soap_Send = findUrl_Soap_Send(url);
        String HttpResponse= Fault_Client_noNS_Begin +
                XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                Fault_noNS_End;;
        int  Interface_id =
                MessageRepositoryHelper.look4MessageTypeVO_2_Interface(Url_Soap_Send, Controller_log);
        Controller_log.warn("Interface_id=" + Interface_id );
        if ( Interface_id < 0 )
        {   getResponse.setStatus(500);
            HttpResponse= Fault_Client_noNS_Begin +
                    "Интерфейс для обработки " +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")" ) +
                    " в системе не сконфигурирован" +
                    Fault_noNS_End ;
            Controller_log.warn("HttpResponse:\n" + HttpResponse);
            try {
                JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                String jsonPrettyPrintString = xmlJSONObj.toString(4);
                Controller_log.warn("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                getResponse.setContentType("text/json;Charset=UTF-8");
                return(jsonPrettyPrintString);

            } catch (JSONException e) {
                System.err.println(e.toString());
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            return HttpResponse;
        }
        else
            // Начинае подготовку к обработке запроса
        {   MessageDetails Message = new MessageDetails();

            Message.XML_Request_Method.append(Parametrs_Begin);
            String queryParams[];
            queryParams = queryString.split("&");
            for (int i= 0 ; i < queryParams.length; i++)
            { // Controller_log.warn( queryParams[i]);
                String ParamElements[] = queryParams[i].split("=");
                Controller_log.warn( ParamElements[0] );

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);

                if (( ParamElements.length>1) && (ParamElements[1] !=null )){
                    Controller_log.warn( queryParams[i].substring(ParamElements[0].length()+1 ));
                    Message.XML_Request_Method.append( queryParams[i].substring(ParamElements[0].length()+1 ) );
                }

                Message.XML_Request_Method.append(OpenTag);
                Message.XML_Request_Method.append(EndTag);
                Message.XML_Request_Method.append(ParamElements[0]);
                Message.XML_Request_Method.append(CloseTag);
            }
            Message.XML_Request_Method.append(Parametrs_End);
            Controller_log.info( Message.XML_Request_Method.toString());

            Message.XML_MsgInput = Envelope_noNS_Begin
                    + Header_noNS_Begin + Header_noNS_End
                    + Body_noNS_Begin
                    + Message.XML_Request_Method.toString()
                    + Body_noNS_End + Envelope_noNS_End
            ;
            // Message.XML_MsgClear.append(Message.XML_MsgInput);

            MessageReceiveTask messageReceiveTask = new MessageReceiveTask( );
            // получив на вход интерфейса (на основе входного URL) ищем для него Шаблон
            int MessageTemplateVOkey= MessageRepositoryHelper.look4MessageTemplate_2_Interface( Interface_id, Controller_log);
            boolean isDebugged= false;
            Long Queue_ID = messageReceiveTask.ProcessInputMessage( Interface_id, Message , MessageTemplateVOkey, isDebugged );

            try {
                if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                    messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                messageReceiveTask.theadDataAccess.Hermes_Connection = null;
            } catch (SQLException e) {
                Controller_log.error("Проблемы с закрытием theadDataAccess соединения " + e.getMessage());
                e.printStackTrace();
            }
            if ( Queue_ID == 0L){
                getResponse.setStatus(200);
                HttpResponse = Body_noNS_Begin +
                        Message.XML_MsgResponse.toString() +
                        Body_noNS_End;
            }
            else {
                if ( Queue_ID > 0L ) {
                    getResponse.setStatus(500);
                    HttpResponse =  Fault_Client_noNS_Begin +
                            XML.escape(Message.MsgReason.toString()) +
                            Fault_noNS_End;
                } else {
                    getResponse.setStatus(500);
                    HttpResponse=  Fault_Server_noNS_Begin +
                            Message.MsgReason.toString() +
                            Fault_noNS_End ;
                }
            }
            getResponse.setStatus(200);
/*
            HttpResponse = Fault_Client_noNS_Begin +
                    XML.escape(httpRequest.getMethod() + ": url= (" + url + ") queryString(" + queryString + ")") +
                    Fault_noNS_End;
*/
            Controller_log.info("HttpResponse:" + HttpResponse);
            getResponse.setHeader("Access-Control-Allow-Origin", "*");
            getResponse.setContentType("text/json;Charset=UTF-8");
            // getResponse.setContentType("text/xml;charset=UTF-8");

            try {
                JSONObject xmlJSONObj = XML.toJSONObject(HttpResponse);

                String jsonPrettyPrintString = xmlJSONObj.toString(4);
                System.out.println("jsonPrettyPrintString:\n" + jsonPrettyPrintString);
                getResponse.setContentType("text/json;Charset=UTF-8");
                if ( isDebugged )
                    messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, jsonPrettyPrintString, Controller_log  );
                try {
                    if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                        messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                    messageReceiveTask.theadDataAccess.Hermes_Connection = null;
                } catch (SQLException SQLe) {
                    Controller_log.error(SQLe.getMessage());
                    Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                    SQLe.printStackTrace();
                }
                return (jsonPrettyPrintString);

            } catch (JSONException e) {
                System.err.println(e.toString());
            }
            getResponse.setContentType("text/xml;charset=UTF-8");
            if ( isDebugged )
                messageReceiveTask.theadDataAccess.doUPDATE_QUEUElog( Message.ROWID_QUEUElog, Message.Queue_Id, HttpResponse, Controller_log  );
            try {
                if ( messageReceiveTask.theadDataAccess.Hermes_Connection != null )
                    messageReceiveTask.theadDataAccess.Hermes_Connection.close();
                messageReceiveTask.theadDataAccess.Hermes_Connection = null;
            } catch (SQLException SQLe) {
                Controller_log.error(SQLe.getMessage());
                Controller_log.error( "Hermes_Connection.close() fault:" + SQLe.getMessage());
                SQLe.printStackTrace();
            }
            return HttpResponse;
        }

    }


}
