package net.plumbing.msgbus.controller;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplate4Perform;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import net.plumbing.msgbus.threads.utils.XMLutils;
import net.plumbing.msgbus.threads.utils.XmlSQLStatement;
import org.apache.commons.lang3.StringUtils;
//import org.apache.http.HttpClientConnection;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.http.conn.HttpClientConnectionManager;
//import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.xlstErrorListener;
import ru.hermes.msgbus.model.*;
import net.plumbing.msgbus.threads.TheadDataAccess;
import ru.hermes.msgbus.threads.utils.*;
import net.plumbing.msgbus.ws.client.core.Security;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;

import javax.xml.transform.TransformerException;

//import XMLchars;
//import static sStackTracе.strInterruptedException;


public class PerfotmInputMessages {

    private DefaultHttpClient client=null;
    private CloseableHttpClient httpClient=null;
    private ThreadSafeClientConnManager ExternalConnectionManager;
    private String ConvXMLuseXSLTerr = "";
    //  org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

    public void setExternalConnectionManager( ThreadSafeClientConnManager externalConnectionManager ) {
        this.ExternalConnectionManager = externalConnectionManager;
    }

    private Security endpointProperties;

    public  long performMessage(MessageDetails Message, MessageQueueVO messageQueueVO, TheadDataAccess theadDataAccess, xlstErrorListener XSLTErrorListener, StringBuilder ConvXMLuseXSLTerr, Logger MessegeReceive_Log) {
        // 1. Получаем шаблон обработки для MessageQueueVO
        String SubSys_Cod = messageQueueVO.getSubSys_Cod();
        int MsgDirection_Id = messageQueueVO.getMsgDirection_Id();
        int Operation_Id = messageQueueVO.getOperation_Id();
        Long Queue_Id = messageQueueVO.getQueue_Id();
        String Queue_Direction = messageQueueVO.getQueue_Direction();
        String AnswXSLTQueue_Direction = Queue_Direction;


        int Max_Retry_Count = 1;
        int Max_Retry_Time = 30;
        String URL_SOAP_Send = "";
        int Function_Result = 0;


        MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "] ищем Шаблон под оперрацию (" + Operation_Id + "), с учетом системы приёмника MsgDirection_Id=" + MsgDirection_Id + ", SubSys_Cod =" + SubSys_Cod);

        // ищем Шаблон под оперрацию, с учетом системы приёмника MessageRepositoryHelper.look4MessageTemplateVO_2_Perform
        int Template_Id = MessageRepositoryHelper.look4MessageTemplateVO_2_Perform(Operation_Id, MsgDirection_Id, SubSys_Cod, MessegeReceive_Log);
        MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "]  Шаблон под оперрацию =" + Template_Id);

        if ( Template_Id < 0 ) {
            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Не нашли шаблон обработки сообщения для комбинации: Идентификатор системы[" + MsgDirection_Id + "] Код подсистемы[" + SubSys_Cod + "]",
                    3101, MessegeReceive_Log);
            return -15L;
        }

        int messageTypeVO_Key = MessageRepositoryHelper.look4MessageTypeVO_2_Perform(Operation_Id, MessegeReceive_Log);
        if ( messageTypeVO_Key < 0 ) {
            MessegeReceive_Log.error("[" + Queue_Id + "] MessageRepositoryHelper.look4MessageTypeVO_2_Perform: Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]");
            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Не нашли тип сообщения для Operation_Id=[" + Operation_Id + "]", 3102, MessegeReceive_Log);
            return -17L;
        }

        Message.MessageTemplate4Perform = new MessageTemplate4Perform(MessageTemplate.AllMessageTemplate.get(Template_Id),
                Queue_Id,
                MessegeReceive_Log
        );
        MessegeReceive_Log.info("[" + Queue_Id + "] MessageTemplate4Perform[" + Message.MessageTemplate4Perform.printMessageTemplate4Perform() );
/*
        CloseableHttpClient SimpleHttpClient = null; // Message.SimpleHttpClient;
        String isProxySet = System.getProperty("http.proxySet");

            SimpleHttpClient = getCloseableHttpClient(
                    Message.sslContext, Message.MessageTemplate4Perform,
                    //Message.MessageTemplate4Perform.getPropTimeout_Read() * 1000,
                    //Message.MessageTemplate4Perform.getPropTimeout_Conn() * 1000,
                    MessegeReceive_Log
            );
        if ( SimpleHttpClient == null) {
            return -22L;
        }
        Message.SimpleHttpClient = SimpleHttpClient;
*/
        switch (Queue_Direction){
            case XMLchars.DirectTEMP:

                if ( Message.MessageTemplate4Perform.getMessageXSD() != null )
                { boolean is_Message_OUT_Valid;
                    is_Message_OUT_Valid = XMLutils.TestXMLByXSD( Queue_Id,  Message.XML_Request_Method.toString(), Message.MessageTemplate4Perform.getMessageXSD(), Message.MsgReason, MessegeReceive_Log );
                    if ( ! is_Message_OUT_Valid ) {
                        MessegeReceive_Log.error("[" + Queue_Id + "] validateXMLSchema: message (" + Message.XML_Request_Method.toString() + ") is not valid for XSD " + Message.MessageTemplate4Perform.getMessageXSD());

                        theadDataAccess.doUPDATE_MessageQueue_Temp2ErrIN(Queue_Id,
                                messageQueueVO.getOperation_Id(),
                                messageQueueVO.getMsgDirection_Id(), messageQueueVO.getSubSys_Cod(),
                                messageQueueVO.getMsg_Type(), messageQueueVO.getMsg_Type_own(),
                                Message.MsgReason.toString(),
                                0L,
                                MessegeReceive_Log);

                        MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO,   Message,  theadDataAccess,
                                "validateXMLSchema: message " + Message.MsgReason.toString() + " XSD" + Message.MessageTemplate4Perform.getMessageXSD() ,
                                null ,  MessegeReceive_Log);
                        // Считаем, что виноват клиент
                        return 13L;
                    }
                }


                Function_Result = MessageUtils.SaveMessage4Input(
                         theadDataAccess,  Queue_Id,  Message,  messageQueueVO , MessegeReceive_Log) ;
                if( Function_Result < 0 ) {
                    MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                            "Не удалось сохранить содержимое сообщения в твблицу очереди:"  + " " + Message.XML_MsgClear.toString()  ,
                            null ,  MessegeReceive_Log);
                    return -19L;
                }
                try {
                    if ( MessageUtils.ProcessingIn_setIN(messageQueueVO, Message, theadDataAccess, MessegeReceive_Log) < 0 ) {
                        Message.MsgReason.append("Не удалось сохранить заголовок сообщения в твблицу очереди");
                        return -21L;
                    }
                }  catch (Exception e_setIN) {
                    MessegeReceive_Log.error("Не удалось сохранить заголовок сообщения в твблицу очереди: ProcessingIn_setIN fault " + e_setIN.getMessage());
                    e_setIN.printStackTrace();
                    MessegeReceive_Log.error( "Не удалось сохранить заголовок сообщения в твблицу очереди:");
                    MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                            "Не удалось сохранить заголовок сообщения в твблицу очереди:"  + e_setIN.getMessage() + " " + Message.XML_MsgClear.toString()  ,
                            e_setIN ,  MessegeReceive_Log);
                    return -23L;
                }

            case XMLchars.DirectIN:

                if ((Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) && ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.JavaClassExeMetod) ) )
                { // 2.1) Это JDBC-обработчик
                    if ( Message.MessageTemplate4Perform.getEnvelopeXSLTExt() != null ) { // 2) EnvelopeXSLTExt !!
                        if ( Message.MessageTemplate4Perform.getEnvelopeXSLTExt().length() > 0 ) {
                            if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessegeReceive_Log.info("["+ Queue_Id +"] Шаблон для XSLTExt-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() + ")");

                            String Passed_Envelope4XSLTExt = null;
                            try {
                                Passed_Envelope4XSLTExt= XMLutils.ConvXMLuseXSLT( Queue_Id,
                                        // Message.XML_MsgClear.toString(),
                                        MessageUtils.PrepareEnvelope4XSLTExt( messageQueueVO,   Message,  MessegeReceive_Log), // Искуственный Envelope/Head/Body + XML_Request_Method
                                        Message.MessageTemplate4Perform.getEnvelopeXSLTExt(),  // через EnvelopeXSLTExt
                                        Message.MsgReason, // результат помещаем сюда
                                        ConvXMLuseXSLTerr,
                                        XSLTErrorListener,
                                        MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                            } catch ( TransformerException exception ) {
                                MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLTExt-преобразователь запроса:{" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() +"}");
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr.toString() + " :" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt(), 3229,
                                          MessegeReceive_Log);
                                return -31L;
                            }
                            if ( Passed_Envelope4XSLTExt.equals(XMLchars.EmptyXSLT_Result))
                            {   MessegeReceive_Log.error("["+ Queue_Id +"] Шаблон для XSLTExt-обработки(" + Message.MessageTemplate4Perform.getEnvelopeXSLTExt() + ")");
                                MessegeReceive_Log.error("["+ Queue_Id +"] Envelope4XSLTExt:" + ConvXMLuseXSLTerr.toString());
                                MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для XSLTExt-обработки " + Message.MsgReason.toString() );
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для XSLTExt-обработки " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3231,
                                          MessegeReceive_Log);
                                return -32L;

                            }

                            final int resultSQL = XmlSQLStatement.ExecuteSQLincludedXML( theadDataAccess, Passed_Envelope4XSLTExt, messageQueueVO, Message, MessegeReceive_Log);
                            if (resultSQL != 0) {
                                MessegeReceive_Log.error("["+ Queue_Id +"] Envelope4XSLTExt:" + ConvXMLuseXSLTerr.toString() );
                                MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка ExecuteSQLinXML:" + Message.MsgReason.toString() );
                                theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка ExecuteSQLinXML: " + Message.MsgReason.toString(), 3231,
                                        MessegeReceive_Log);
//                                    try {  SimpleHttpClient.close(); } catch ( IOException e) {
//                                        MessegeReceive_Log.error("и еще ошибка SimpleHttpClient.close(): " + e.getMessage() );
//                                        return -13L;      }
                                return -33L;
                            }
                            else
                            {if ( Message.MessageTemplate4Perform.getIsDebugged() )
                                MessegeReceive_Log.info("["+ Queue_Id +"] Исполнение ExecuteSQLinXML:" + Message.MsgReason.toString() );
                            }

                        }
                        else
                        {
                            // Нет EnvelopeXSLTPost - надо орать! прописан Java класс, а EnvelopeXSLTPost нет
                            MessegeReceive_Log.error("["+ Queue_Id +"] В шаблоне для XSLTExt-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет Envelope4XSLTExt");
                            theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,
                                    "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет Envelope4XSLTExt", 3232,
                                      MessegeReceive_Log);
//                                try {  SimpleHttpClient.close(); } catch ( IOException e) {
//                                    MessegeReceive_Log.error("и еще ошибка SimpleHttpClient.close(): " + e.getMessage() );
//                                    return -14L;      }
                            return -34L;
                        }
                    }
                    else
                    {
                        // Нет EnvelopeXSLTPost - надо орать!
                        MessegeReceive_Log.error("["+ Queue_Id +"] В шаблоне для XSLTExt-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет Envelope4XSLTExt");
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,
                                "В шаблоне для пост-обработки " + Message.MessageTemplate4Perform.getPropExeMetodPostExec() + " нет Envelope4XSLTExt", 3233,
                                 MessegeReceive_Log);
                        return -35L;
                    }
                }

                MessegeReceive_Log.warn("["+ Queue_Id +"] В шаблоне для синхронной обработки getIsDebugged="+ Message.MessageTemplate4Perform.getIsDebugged() +
                                     " getPropExeMetodExecute= " + Message.MessageTemplate4Perform.getPropExeMetodExecute() );
                if ((Message.MessageTemplate4Perform.getPropExeMetodExecute() != null) && ( Message.MessageTemplate4Perform.getPropExeMetodExecute().equals(Message.MessageTemplate4Perform.WebRestExeMetod)) )
                { // 2.2) Это Rest-HttpGet-вызов
                    ;
                    if (( Message.MessageTemplate4Perform.getPropHost() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropUser() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropPswd() == null ) ||
                            ( Message.MessageTemplate4Perform.getPropUrl()  == null ) )
                    {
                        // Нет параметров для Rest-HttpGet - надо орать!
                        Message.MsgReason.append( "В шаблоне для синхронной-обработки " + Message.MessageTemplate4Perform.getPropExeMetodExecute() + " нет параметров для Rest-HttpGet вклюая логин/пароль");
                        MessegeReceive_Log.error("["+ Queue_Id +"] " + Message.MsgReason.toString());
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3242,
                               MessegeReceive_Log);
                        return -36L;
                    }
                    String EndPointUrl= null;
                    try {

                        if ( StringUtils.substring(Message.MessageTemplate4Perform.getPropHost(),0,"http".length()).equalsIgnoreCase("http") )
                            EndPointUrl =
                                    Message.MessageTemplate4Perform.getPropHost() +
                                            Message.MessageTemplate4Perform.getPropUrl();
                        else
                            EndPointUrl = "http://" + Message.MessageTemplate4Perform.getPropHost() +
                                    Message.MessageTemplate4Perform.getPropUrl();
                        // Ставим своенго клиента ! ?
                        Unirest.setHttpClient( Message.RestHermesAPIHttpClient);
                        String RestResponse =
                                Unirest.get(EndPointUrl)
                                        .queryString("queue_id", Queue_Id.toString())
                                        .basicAuth(Message.MessageTemplate4Perform.getPropUser(),
                                                Message.MessageTemplate4Perform.getPropPswd())
                                        .asString().getBody();
                        //if ( Message.MessageTemplate4Perform.getIsDebugged() )
                            MessegeReceive_Log.info("[" + messageQueueVO.getQueue_Id() + "]" +"MetodExec.Unirest.get(" + EndPointUrl + ") RestResponse=(" + RestResponse + ")");
                    } catch ( UnirestException e) {
                        e.printStackTrace();
                        // возмущаемся,
                        Message.MsgReason.append("Ошибка синхронного вызова обработчика HttpGet(" + EndPointUrl + "):" + e.toString() );
                        MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка синхронного вызова обработчика HttpGet(" + EndPointUrl + "):" + e.toString() );
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id,"Ошибка синхронного вызова обработчика HttpGet(" + EndPointUrl + "):" + e.toString(), 3244,
                                MessegeReceive_Log);
                        return -37L;
                    }
                }


                // Проверяем готовность результата
                if ( MessageUtils.isMessageQueue_Direction_EXEIN( theadDataAccess,  Queue_Id,  messageQueueVO,  MessegeReceive_Log) ) {
                    // Если статус EXEIN , сообщение выполнено, либо нормально либо с ошибкой смотрим на Confirmation
                    int ConfirmationRowNum= MessageUtils.ReadConfirmation(theadDataAccess, Queue_Id, Message, MessegeReceive_Log);
                    if ( ConfirmationRowNum < 1) {
                        // Ругаемся, что обработчик не сформировал Confirmation
                        Message.MsgReason.append("обработчик не сформировал Confirmation, нарушено соглашение о взаимодействии с Шиной");
                        MessegeReceive_Log.error("["+ Queue_Id +"] " + Message.MsgReason.toString());
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3245,
                                MessegeReceive_Log);
                        return -38L;
                    }
                }
                else {
                    // Ругаемся, что обработчик не выставил признак статус EXEIN
                    Message.MsgReason.append("обработчик не выставил признак статус EXEIN , нарушено соглашение о взаимодействии с Шиной");
                    MessegeReceive_Log.error("["+ Queue_Id +"] " + Message.MsgReason.toString());
                    theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, Message.MsgReason.toString(), 3247,
                            MessegeReceive_Log);
                    return -39L;
                }
                MessegeReceive_Log.warn("преобразовываем результат getAckXSLT( " + Message.MessageTemplate4Perform.getAckXSLT() +")");

                // преобразовываем результат
                if (Message.MessageTemplate4Perform.getAckXSLT() != null)
                {
                    String Passed_Confirmation4AckXSLT = null;
                    try {
                        Passed_Confirmation4AckXSLT= XMLutils.ConvXMLuseXSLT( Queue_Id,
                                Message.XML_MsgConfirmation.toString(), //
                                Message.MessageTemplate4Perform.getAckXSLT(),  // через AckXSLT
                                Message.MsgReason, // результат для MsgReason помещаем сюда
                                ConvXMLuseXSLTerr,
                                XSLTErrorListener,
                                MessegeReceive_Log, Message.MessageTemplate4Perform.getIsDebugged());
                    } catch ( TransformerException exception ) {
                        MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLTExt-преобразователь Confirmation:{" + Message.MessageTemplate4Perform.getAckXSLT() +"}");
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MessageTemplate4Perform.getAckXSLT(), 3249,
                                MessegeReceive_Log);
                        return -41L;
                    }
                    if ( Passed_Confirmation4AckXSLT.equals(XMLchars.EmptyXSLT_Result))
                    {   MessegeReceive_Log.error("["+ Queue_Id +"] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getAckXSLT() + ")");
                        MessegeReceive_Log.error("["+ Queue_Id +"] Passed_Confirmation4AckXSLT:" + ConvXMLuseXSLTerr.toString());
                        MessegeReceive_Log.error("["+ Queue_Id +"] Ошибка преобразования XSLT для обработки Confirmation" + Message.MsgReason.toString() );
                        theadDataAccess.doUPDATE_MessageQueue_In2ErrorIN(Queue_Id, "Ошибка преобразования XSLT для обработки Confirmation " + ConvXMLuseXSLTerr.toString() + " :" + Message.MsgReason.toString(), 3251,
                                MessegeReceive_Log);
                        return -42L;

                    }
                    Message.XML_MsgResponse.append(Passed_Confirmation4AckXSLT.substring(XMLchars.xml_xml.length()));
                    MessegeReceive_Log.warn("["+ Queue_Id +"] возврашаем(" + Message.XML_MsgResponse + ")");
                }
                else // Помещаем Без преобразования
                {
                    MessegeReceive_Log.warn("["+ Queue_Id +"] Шаблон для XSLT-обработки Confirmation(" + Message.MessageTemplate4Perform.getAckXSLT() + ")");
                    Message.XML_MsgResponse.append(Message.XML_MsgConfirmation.toString());
                }


                // Устанавливаеи признак завершения работы
                theadDataAccess.doUPDATE_MessageQueue_ExeIn2DelIN(Queue_Id, MessegeReceive_Log );

                break;
        }


            return  0L;
    }

/*
    public CloseableHttpClient getCloseableHttpClient( SSLContext sslContext, MessageTemplate4Perform messageTemplate4Perform,
                                                       Logger getHttpClient_Log) {
        Integer SocketTimeout =  messageTemplate4Perform.getPropTimeout_Read() * 1000;
        Integer ConnectTimeout = messageTemplate4Perform.getPropTimeout_Conn() * 1000;

        if ( this.client == null ) {
            this.client = new DefaultHttpClient( this.ExternalConnectionManager );
        }
        HttpParams httpParameters = new BasicHttpParams();

        HttpConnectionParams.setConnectionTimeout(httpParameters, ConnectTimeout) ; // connectTimeoutInMillis);
        HttpConnectionParams.setSoTimeout(httpParameters, SocketTimeout );  //readTimeoutInMillis;
        HttpHost proxyHost;
        String isProxySet = System.getProperty("http.proxySet");
        getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxySet\") [" + isProxySet + "]");
        if ( (isProxySet != null) && (isProxySet.equals("true")) ) {
            getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxyHost\") [" + System.getProperty("http.proxyHost") + "]");
            getHttpClient_Log.info("getCloseableHttpClient(): System.getProperty(\"http.proxyPort\") [" + System.getProperty("http.proxyPort") + "]");
            proxyHost = new HttpHost( System.getProperty("http.proxyHost"), Integer.parseInt(System.getProperty("http.proxyPort")) );
            ConnRouteParams.setDefaultProxy(httpParameters, proxyHost );
            getHttpClient_Log.info("ConnRouteParams.DEFAULT_PROXY[" + ConnRouteParams.DEFAULT_PROXY + "]");
        }

        this.client.setParams(httpParameters);
        String EndPointUrl;

        if ( StringUtils.substring(messageTemplate4Perform.getEndPointUrl(),0,"http".length()).equalsIgnoreCase("http") )
            EndPointUrl = messageTemplate4Perform.getEndPointUrl();
        else
            EndPointUrl = "http://" + messageTemplate4Perform.getEndPointUrl();
        if ( StringUtils.substring(EndPointUrl,0,"https".length()).equalsIgnoreCase("https") ) {
            SSLSocketFactory factory;
            int port;
            endpointProperties = Security.builder().build();
            URI Uri_4_Request;
            try {
                try {
                    Uri_4_Request = new URI(EndPointUrl);

                } catch (URISyntaxException ex) {
                    throw new SoapClientException(String.format("URI [%s] is malformed", EndPointUrl) + ex.getMessage(), ex);
                }
                factory = SSLUtils.getFactory(endpointProperties);
                port = Uri_4_Request.getPort();
                registerTlsScheme(factory, port);

            } catch (GeneralSecurityException ex) {
                throw new SoapClientException(ex);
            }
        }
        return  this.client;

    }
    private void registerTlsScheme(SchemeLayeredSocketFactory factory, int port) {
        Scheme sch = new Scheme(HTTPS, port, factory);
        client.getConnectionManager().getSchemeRegistry().register(sch);
    }
*/

}
