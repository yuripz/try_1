package net.plumbing.msgbus.controller;

import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.threads.TheadDataAccess;
import net.plumbing.msgbus.threads.utils.MessageHttpSend;
import net.plumbing.msgbus.threads.utils.MessageUtils;
import net.plumbing.msgbus.threads.utils.XMLutils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
//import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
//import org.jdom2.JDOMException;
//import org.jdom2.input.JDOMParseException;
import org.jdom2.input.JDOMParseException;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import ru.hermes.msgbus.model.*;
// import ru.hermes.msgbus.SenderApplication;

import javax.net.ssl.SSLContext;
//import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;

//import ApplicationProperties;


@Component
@Scope("prototype")
//@Configuration
//@Bean (name="MessageSendTask")
public class MessageReceiveTask
{
    //public Connection Hermes_Connection;
    private   String name;
    public static final Logger MessegeReceive_Log = LoggerFactory.getLogger(MessageReceiveTask.class);

    // private ThreadSafeClientConnManager externalConnectionManager;
    private xlstErrorListener XSLTErrorListener=null;
    public TheadDataAccess theadDataAccess=null;



   // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public Long  ProcessInputMessage(Integer Interface_id , MessageDetails Message, int MessageTemplateVOkey, boolean isDebugged) {

        XSLTErrorListener = new xlstErrorListener();
        StringBuilder ConvXMLuseXSLTerr = new StringBuilder(); ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        XSLTErrorListener.setXlstError_Log( MessegeReceive_Log );
        final  String Queue_Direction="ProcessInputMessage";
        Long Queue_Id = -1L;
        Long Function_Result = 0L;

        // MessageTemplateVOkey - Шаблон интерфейса (на основе входного URL)
        try {
            if ( MessageTemplateVOkey >= 0 )
            // Парсим входной запрос и формируем XML-Вщсгьуте !
            XMLutils.makeClearRequest(Message, MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getEnvelopeInXSLT(),
                    XSLTErrorListener, ConvXMLuseXSLTerr,
                    MessegeReceive_Log);
            else
                XMLutils.makeClearRequest(Message, null,
                        XSLTErrorListener, ConvXMLuseXSLTerr,
                        MessegeReceive_Log);
        }
        catch (Exception e) {
            System.err.println( "["+ Message.XML_MsgInput + "]  Exception" );
            e.printStackTrace();
            MessegeReceive_Log.error(Queue_Direction + "fault: [" + Message.XML_MsgInput + "] XMLutils.makeClearRequest fault: " + sStackTracе.strInterruptedException(e));
            Message.MsgReason.append("Ошибка на приёме сообщения: " + e.getMessage() ); //  sStackTracе.strInterruptedException(e));
               if ( (e instanceof JDOMParseException ) || (e instanceof XPathExpressionException)  ) // Клиент прислсл фуфло
                   return 1L;
            else
                   return -1L;

        }
        if ( isDebugged )
        MessegeReceive_Log.info("Clear request:" + Message.XML_MsgClear.toString() );

        MessageQueueVO messageQueueVO = new MessageQueueVO();

        // TheadDataAccess
                this.theadDataAccess = new TheadDataAccess();
        // Установаливем " соединение" , что бы зачитывать очередь
        if ( isDebugged )
        MessegeReceive_Log.info("Установаливем \"соединение\" , что бы зачитывать очередь: [" +
                ApplicationProperties.HrmsPoint + "] user:" + ApplicationProperties.hrmsDbLogin +
                "; passwd:" + ApplicationProperties.hrmsDbPasswd + ".");
        theadDataAccess.make_Hikari_Connection(
                ApplicationProperties.hrmsDbLogin, ApplicationProperties.hrmsDbPasswd,
                ApplicationProperties.dataSource,
                MessegeReceive_Log
                );
        if ( theadDataAccess.Hermes_Connection == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения - theadDataAccess.make_Hikari_Connection return: NULL!"  );
            return -2L;
        }

        // Создаем запись в таблице-очереди  select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL ...
        Queue_Id = MessageUtils.MakeNewMessage_Queue( messageQueueVO, theadDataAccess, MessegeReceive_Log );
        if ( Queue_Id == null ){
            Message.MsgReason.append("Ошибка на приёме сообщения, не удалось сохранить заголовок сообщения в БД - MakeNewMessage_Queue return: " + Queue_Id );
            return -3L;
        }
        Message.ROWID_QUEUElog=null; Message.Queue_Id = Queue_Id;
        if ( isDebugged )
            Message.ROWID_QUEUElog = theadDataAccess.doINSERT_QUEUElog( Queue_Id , Message.XML_MsgInput, MessegeReceive_Log );


        if ( MessageTemplateVOkey >= 0 )
        {  // Получаем Шаблон формирования заголовка для этого интерфейса HeaderInXSLT
            String MessageXSLT_4_HeaderIn = MessageTemplate.AllMessageTemplate.get( MessageTemplateVOkey ).getHeaderInXSLT();
            // При наличии в шаблоне  интерфейса файла преобразования HeaderInXSLT,
            //  к исходному запросу(SOAP-Envelope) с удаленной информацией о NameSpace
            //  применяется преобразование HeaderInXSLT и полученный результат
            //  заменяет SOAP-Header запроса, вне зависимости от того был он или нет.
            if ( MessageXSLT_4_HeaderIn != null )
            {
                ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
                try {
                    Message.Soap_HeaderRequest.append(XMLutils.ConvXMLuseXSLT(Queue_Id,
                            Message.XML_MsgClear.toString(),
                            MessageXSLT_4_HeaderIn,
                            Message.MsgReason,
                            ConvXMLuseXSLTerr,
                            XSLTErrorListener,
                            MessegeReceive_Log,
                            true
                            ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                    );
                    //if ( Message.MessageTemplate4Perform.getIsDebugged() )
                    if ( isDebugged )
                    MessegeReceive_Log.info(Queue_Direction + " [" + Queue_Id + "] после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    if ( Message.Soap_HeaderRequest.toString().equals(XMLchars.nanXSLT_Result) ) {
                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess, "В результате XSLT преобразования получен пустой заголовок из (" + Message.XML_MsgClear.toString() + ")",
                                null, MessegeReceive_Log);
                        Message.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                        return -5L;
                    }

                } catch (TransformerException exception) {
                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] XSLT-преобразователь тела:{" + MessageXSLT_4_HeaderIn + "}");
                    MessegeReceive_Log.error(Queue_Direction + " [" + Queue_Id + "] fault " + ConvXMLuseXSLTerr.toString() + " после XSLT=:{" + Message.Soap_HeaderRequest.toString() + "}");
                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " + ConvXMLuseXSLTerr + Message.XML_MsgClear.toString() + " on " + MessageXSLT_4_HeaderIn,
                            null, MessegeReceive_Log);
                    Message.MsgReason.append("Ошибка построенния заголовка при XSLT-преобразовании из сообщения: " +  ConvXMLuseXSLTerr.toString());
                    // Считаем, что виноват клиент
                    return 5L;
                }

                try { // Парсим заголовок - получаем атрибуты messageQueueVO для сохранения в БД
                    XMLutils.Soap_HeaderRequest2messageQueueVO(Message.Soap_HeaderRequest.toString(), messageQueueVO, MessegeReceive_Log);

                }
                catch (Exception e) {
                    System.err.println( "Queue_Id["+ messageQueueVO.getQueue_Id() + "]  Exception" );
                    e.printStackTrace();
                    MessegeReceive_Log.error(Queue_Direction + "fault: [" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: " + sStackTracе.strInterruptedException(e));
                    Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString() + ", fault: " + sStackTracе.strInterruptedException(e));

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения: " + Message.XML_MsgClear.toString(),
                            e, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 7L;

                }

            }
            else { // Берем распарсенный  Context из Header сообщения для сохранения в БД
                if ( Message.Input_Header_Context != null ) {
                    try {
                        XMLutils.Soap_XMLDocument2messageQueueVO(Message.Input_Header_Context, messageQueueVO, MessegeReceive_Log);
                    } catch (Exception e) {
                        System.err.println("Queue_Id [" + messageQueueVO.getQueue_Id() + "]  Exception");
                        e.printStackTrace();
                        MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_HeaderRequest2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " + sStackTracе.strInterruptedException(e));
                        Message.MsgReason.append("Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Queue_Direction + ", fault: " + sStackTracе.strInterruptedException(e));

                        MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                                "Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                                e, MessegeReceive_Log);
                        // Считаем, что виноват клиент
                        return 9L;

                    }
                }
                else {
                    MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "Soap_XMLDocument2messageQueueVO: (" +  Message.XML_MsgClear.toString() + ") fault " );
                    Message.MsgReason.append("Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, построенного XSLT из сообщения (: " + Message.XML_MsgClear.toString() + ")" );

                    MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                            "Не был найдет элемент 'Context' - Ошибка при получении необходимых значений из заголовка, полученного в сообщении: " + Message.XML_MsgClear.toString(),
                            null, MessegeReceive_Log);
                    // Считаем, что виноват клиент
                    return 11L;
                }
            }

        }


        Message.XML_MsgResponse.setLength(0); Message.XML_MsgResponse.trimToSize();


            int ReadTimeoutInMillis = ApplicationProperties.ApiRestWaitTime * 1000;
            int ConnectTimeoutInMillis = 5 * 1000;
            SSLContext sslContext = MessageHttpSend.getSSLContext( Message.MsgReason );
            if ( sslContext == null ) {
                MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")");
                Message.MsgReason.append("Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")" ) ;

                MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                        "Внутренняя Ошибка SSLContextBuilder fault: (" +  Message.MsgReason.toString() + ")",
                        null, MessegeReceive_Log);
                return -7L;
            }
            PoolingHttpClientConnectionManager syncConnectionManager = new PoolingHttpClientConnectionManager();
            syncConnectionManager.setMaxTotal((Integer) 4);
            syncConnectionManager.setDefaultMaxPerRoute((Integer) 2);

            //externalConnectionManager = new ThreadSafeClientConnManager();
            //externalConnectionManager.setMaxTotal((Integer) 99);
            //externalConnectionManager.setDefaultMaxPerRoute((Integer) 99);


            RequestConfig rc;

            rc = RequestConfig.custom()
                    .setConnectionRequestTimeout(ConnectTimeoutInMillis)
                    .setConnectTimeout(ConnectTimeoutInMillis)
                    .setSocketTimeout( ReadTimeoutInMillis)
                    .build();

            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create()
                    .disableDefaultUserAgent()
                    .disableRedirectHandling()
                    .disableAutomaticRetries()
                    .setUserAgent("Mozilla/5.0")
                    .setSSLContext(sslContext)
                    .disableAuthCaching()
                    .disableConnectionState()
                    .disableCookieManagement()
                    // .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                    .setConnectionManager(syncConnectionManager)
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
            .setConnectionTimeToLive( ApplicationProperties.ApiRestWaitTime + 5, TimeUnit.SECONDS)
            .evictIdleConnections((long) (ApplicationProperties.ApiRestWaitTime + 5)*2, TimeUnit.SECONDS);
            httpClientBuilder.setDefaultRequestConfig(rc);

            CloseableHttpClient
                    ApiRestHttpClient = httpClientBuilder.build();
            if ( ApiRestHttpClient == null) {
                try {
                    syncConnectionManager.shutdown();
                    syncConnectionManager.close();
                } catch ( Exception e) {
                    MessegeReceive_Log.error( "Внутренняя ошибка - httpClientBuilder.build() не создал клиента. И ещё проблема с syncConnectionManager.shutdown()...");
                    e.printStackTrace();
                }
                MessegeReceive_Log.error("[" + messageQueueVO.getQueue_Id() + "]" + "httpClientBuilder.build() fault");
                Message.MsgReason.append("Внутренняя Ошибка httpClientBuilder.build() fault");

                MessageUtils.ProcessingIn2ErrorIN(messageQueueVO, Message, theadDataAccess,
                        "Внутренняя Ошибка httpClientBuilder.build() fault",
                        null, MessegeReceive_Log);
                return -9L;
            }

        PerfotmInputMessages Perfotmer = new PerfotmInputMessages();
        Message.ReInitMessageDetails( sslContext, httpClientBuilder, null, ApiRestHttpClient );
            try {

                // Обрабатываем сообщение!
                Function_Result = Perfotmer.performMessage(Message, messageQueueVO, theadDataAccess,
                                                           XSLTErrorListener,  ConvXMLuseXSLTerr,  MessegeReceive_Log );

        }
        catch (Exception e) {
            System.err.println( "performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            e.printStackTrace();
            MessegeReceive_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " +e.getMessage());
            MessegeReceive_Log.error( "что то пошло совсем не так...");
            MessageUtils.ProcessingIn2ErrorIN(  messageQueueVO, Message,  theadDataAccess,
                    "SaveMessage4Input.SAXBuilder fault:"  + e.getMessage() + " " + Message.XML_MsgClear.toString()  ,
                    null ,  MessegeReceive_Log);
            try {
                ApiRestHttpClient.close();
                syncConnectionManager.shutdown();
                syncConnectionManager.close();
            } catch ( IOException IOe) {
                MessegeReceive_Log.error( "И ещё проблема с ApiRestHttpClient.close()/syncConnectionManager.shutdown()...");
                IOe.printStackTrace();
            }
            return -11L;
        }
        try {
            ApiRestHttpClient.close();
            syncConnectionManager.shutdown();
            syncConnectionManager.close();
        } catch ( IOException e) {
            MessegeReceive_Log.error( "И ещё проблема с ApiRestHttpClient.close()...");
            e.printStackTrace();
        }


/*
            for ( theadRunCount = 0; theadRunCount < theadRunTotalCount; theadRunCount += 1 ) {
                long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
                if ( secondsFromEpoch - startTimestamp > Long.valueOf(60L * TotalTimeTasks) )
                    break;
                else
                    theadRunTotalCount += 1;
                try {
                    try {
                        ResultSet rs = stmtMsgQueue.executeQuery();
                        while (rs.next()) {
                            messageQueueVO.setMessageQueue(
                                    rs.getLong("Queue_Id"),
                                    rs.getLong("Queue_Date"),
                                    rs.getLong("OutQueue_Id"),
                                    rs.getLong("Msg_Date"),
                                    rs.getInt("Msg_Status"),
                                    rs.getInt("MsgDirection_Id"),
                                    rs.getInt("Msg_InfoStreamId"),
                                    rs.getInt("Operation_Id"),
                                    rs.getString("Queue_Direction"),
                                    rs.getString("Msg_Type"),
                                    rs.getString("Msg_Reason"),
                                    rs.getString("Msg_Type_own"),
                                    rs.getString("Msg_Result"),
                                    rs.getString("SubSys_Cod"),
                                    rs.getString("Prev_Queue_Direction"),
                                    rs.getInt("Retry_Count"),
                                    rs.getLong("Prev_Msg_Date"),
                                    rs.getLong("Queue_Create_Date")
                            );

                            MessegeReceive_Log.info( "messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") +
                                    " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod"));
                            // Очистили Message от всего, что там было
                            Message.ReInitMessageDetails( sslContext, httpClientBuilder, null, ApiRestHttpClient );
                            try {
                                PerfotmOUTMessages.performChildOutMessage(Message, messageQueueVO, TheadDataAccess, MessegeReceive_Log );
                            }
                            catch (Exception e) {
                                System.err.println( "performMessage Exception Queue_Id:[" + rs.getLong("Queue_Id") + "] " +e.getMessage());
                                e.printStackTrace();
                                MessegeReceive_Log.error("performMessage Exception Queue_Id:[" + rs.getLong("Queue_Id") + "] " +e.getMessage());
                                MessegeReceive_Log.error( "что то пошло совсем не так...");
                            }
                        }
                        rs.close();
                    } catch (Exception e) {
                        MessegeReceive_Log.error(e.getMessage());
                        e.printStackTrace();
                        MessegeReceive_Log.error( "что то пошло совсем не так...");
                        return;
                    }

                    MessegeReceive_Log.info( "Ждём'c; в " + theadRunCount + " раз " +  WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp ) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch
                            // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
                    );
                    Thread.sleep( WaitTimeBetweenScan* 1000);

                } catch (InterruptedException e) {
                    MessegeReceive_Log.error("MessageSendTask[" + theadNum + "]: is interrapted: " + e.getMessage());
                    e.printStackTrace();
                }

             //   MessegeReceive_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");

            }
            */
            // MessegeReceive_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
            // this.SenderExecutor.shutdown();

        return Function_Result.longValue(); // messageQueueVO.getQueue_Id();
    }


}
