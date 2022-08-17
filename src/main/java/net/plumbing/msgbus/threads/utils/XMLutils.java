package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.common.xlstErrorListener;
import net.plumbing.msgbus.model.MessageDetails;
import net.plumbing.msgbus.model.MessageDirections;
import net.plumbing.msgbus.model.MessageQueueVO;
import net.plumbing.msgbus.model.MessageType;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.XMLOutputter;
import org.jdom2.output.Format;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.xml.XMLConstants;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.OutputKeys;

import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;


import net.plumbing.msgbus.common.XMLchars;

public class XMLutils {

    public static int Soap_XMLDocument2messageQueueVO(@NotNull Element Header_Context, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException
    {
      int parseResult;
            if ( Header_Context == null  ){
                MessegeSend_Log.error("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.Header + ". Header_Context == null" );
                throw new XPathExpressionException("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext);
            }

            if ( Header_Context.getName().equals(XMLchars.TagContext) )
            {
                parseResult = makeMessageQueueVO_from_ContextElement( Header_Context, messageQueueVO, MessegeSend_Log  );
            }
            else {
                MessegeSend_Log.error("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext );
                throw new XPathExpressionException("Soap_XMLDocument2messageQueueVO: в SOAP-запросе не найден Element=" + XMLchars.TagContext);
            }

      return parseResult;
    }

    public static int makeMessageQueueVO_from_ContextElement( Element hContext, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException{

        if ( hContext.getName().equals(XMLchars.TagContext) ) {
            String EventInitiator="";
            String EventSource="";
            Integer EventOperationId=null;
            Long EventKey= messageQueueVO.getQueue_Id();

            List<Element> list = hContext.getChildren();
            // Перебор всех элементов Context
            for (int i = 0; i < list.size(); i++) {
                Element XMLelement = (Element) list.get(i);
                String ElementEntry = XMLelement.getName();
                String ElementContent = XMLelement.getText() ;

                switch ( ElementEntry) {
                    case XMLchars.TagEventInitiator:
                        EventInitiator = ElementContent;
                        break;
                    case XMLchars.TagEventSource:
                        EventSource = ElementContent;
                        break;
                    case XMLchars.TagEventOperationId:
                        EventOperationId = Integer.parseInt(ElementContent);
                        break;
                    case XMLchars.TagEventKey:
                        EventKey = Long.parseLong(ElementContent);
                        if ( EventKey.longValue() == -1L) EventKey = messageQueueVO.getQueue_Id();
                        break;
                }
            }

            int MsgDirectionVO_Key =
                    MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventInitiator, MessegeSend_Log );
            if ( MsgDirectionVO_Key >= 0 ) {
                messageQueueVO.setEventInitiator( MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                        MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());
            }
            else {
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventInitiator + ") объявлен неизвестый код системы-инициатора " + EventInitiator);
            }

            MsgDirectionVO_Key =
                    MessageRepositoryHelper.look4MessageDirectionsVO_2_MsgDirection_Cod( EventSource, MessegeSend_Log );
            if ( MsgDirectionVO_Key >= 0 ) {
                messageQueueVO.setEventInitiator( MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id(),
                        MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod());
                MessegeSend_Log.info("Нашли [" + MsgDirectionVO_Key + "] MsgDirection_Id (" +
                        MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getMsgDirection_Id() +
                        ") Subsys_Cod(" + MessageDirections.AllMessageDirections.get(MsgDirectionVO_Key).getSubsys_Cod() + ")" );
            }
            else {
                MessegeSend_Log.error("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventSource + ") объявлен неизвестый код системы-источника " + EventSource);
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventSource + ") объявлен неизвестый код системы-источника " + EventSource);
            }
            int MessageTypeVO_Key =
                    MessageRepositoryHelper.look4MessageTypeVO_2_Perform(  EventOperationId , MessegeSend_Log);
            if ( MessageTypeVO_Key >= 0 ) {
                messageQueueVO.setMsg_Type( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() );
                messageQueueVO.setMsg_Type_own( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own() );
                messageQueueVO.setOperation_Id( EventOperationId );
                messageQueueVO.setOutQueue_Id( EventKey );
                messageQueueVO.setMsg_Reason( MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() + "() Ok." );

                MessegeSend_Log.info("Нашли [" + MessageTypeVO_Key + "] Msg_Type(" +
                        MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type() +
                        ") Msg_Type_ow(" + MessageType.AllMessageType.get(MessageTypeVO_Key).getMsg_Type_own() + ")" );
            }
            else {
                MessegeSend_Log.error( "getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventOperationId + ") объявлен неизвестый № операцмм " + EventOperationId);
                throw new XPathExpressionException("getClearRequest: в SOAP-заголовке (" + XMLchars.TagEventOperationId + ") объявлен неизвестый № операцмм " + EventOperationId);
            }
        }
        else {
            MessegeSend_Log.error("getClearRequest: в SOAP-заголовке не найден " + XMLchars.TagContext);
            throw new XPathExpressionException("getClearRequest: в SOAP-заголовке не найден " + XMLchars.TagContext);
        }
        MessegeSend_Log.debug( "makeMessageQueueVO_from_ContextElement: " + messageQueueVO.toSring() );
      return 0;
    }


    public static int Soap_HeaderRequest2messageQueueVO(@NotNull String Soap_HeaderRequest, MessageQueueVO messageQueueVO, Logger MessegeSend_Log)
            throws JDOMException, IOException, XPathExpressionException
    {

        int parseResult = 0;
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(Soap_HeaderRequest.getBytes(StandardCharsets.UTF_8));
        Document Soap_HeaderDocument = null;
        try {
            Soap_HeaderDocument =  documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMParseException e)
        {
            MessegeSend_Log.error("Soap_HeaderRequest2messageQueueVO: documentBuilder.build (" + Soap_HeaderRequest + ")fault"  );
            throw new JDOMParseException("client.post:Soap_HeaderRequest2messageQueueVO=(" + Soap_HeaderRequest+ ")", e);
        }

        //MessegeSend_Log.info("documentBuilder.build for (" + Soap_HeaderRequest + ")"  );

        Element hContext = Soap_HeaderDocument.getRootElement();
        parseResult = makeMessageQueueVO_from_ContextElement( hContext, messageQueueVO, MessegeSend_Log  );

        MessegeSend_Log.warn( "Soap_HeaderRequest2messageQueueVO: " + messageQueueVO.toSring() );

        return parseResult;
    }

    public static String makeClearRequest(@NotNull MessageDetails messageDetails, String pEnvelopeInXSLT,
                                          xlstErrorListener XSLTErrorListener, StringBuilder ConvXMLuseXSLTerr,
                                          Logger MessegeSend_Log)
            throws JDOMParseException, JDOMException, IOException, XPathExpressionException, TransformerException
    {
        SAXBuilder documentBuilder = new SAXBuilder();
        //DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream parsedConfigStream = new ByteArrayInputStream(messageDetails.XML_MsgInput.getBytes(StandardCharsets.UTF_8));
        Document document = null;
        try {
            document =  documentBuilder.build(parsedConfigStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMParseException JDOMe)
        {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgInput + ")fault"  );
            throw new JDOMParseException(JDOMe.getMessage()+ ": makeClearRequest=(" + messageDetails.XML_MsgInput + ")" , JDOMe );
        }

        // 1й проход - очищаем входной XML от Ns:
        // MessegeSend_Log.info("documentBuilder.build for (" + messageDetails.XML_MsgInput + ")"  );
        Element SoapEnvelope = document.getRootElement();
        // MessegeSend_Log.error("debug HE-5865: SoapEnvelope.getName()= (" + SoapEnvelope.getName() + ")"  );
        // надо подготовить очищенный от ns: содержимое Envelope.
        // Это всё в конструкторе messageDetails.Message.clear();messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();

        if ( SoapEnvelope.getName().equals(XMLchars.Envelope) ) {
            MessegeSend_Log.warn("debug HE-5865: SoapEnvelope.getName()= (" + SoapEnvelope.getName() + ")"  );
            messageDetails.XML_MsgClear.append(XMLchars.Envelope_noNS_Begin );
            SoapBody2XML_String(messageDetails, SoapEnvelope, MessegeSend_Log);
            messageDetails.XML_MsgClear.append(XMLchars.Envelope_noNS_End );
        } else {
            throw new XPathExpressionException("getClearRequest: в SOAP-запросе не найден RootElement=" + XMLchars.Envelope);
        }
        // MessegeSend_Log.warn("Парсим, 1й проход: XML_MsgClear= (" + messageDetails.XML_MsgClear.toString() + ")"  );

        if ( pEnvelopeInXSLT != null ) {
        //    в интерфейсном шаблоне обозначено преобразование, которое надо исполнить над XML_MsgClear
            ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();

                messageDetails.XML_MsgConfirmation.append(
                        XMLutils.ConvXMLuseXSLT(-1L,
                                messageDetails.XML_MsgClear.toString(),
                                pEnvelopeInXSLT,
                                messageDetails.MsgReason,
                        ConvXMLuseXSLTerr,
                        XSLTErrorListener,
                                MessegeSend_Log,
                        true
                        ).substring(XMLchars.xml_xml.length()) // берем после <?xml version="1.0" encoding="UTF-8"?>
                );
            MessegeSend_Log.info("ProcessInputMessage: после XSLT={" + messageDetails.XML_MsgConfirmation.toString() + "}");
            if ( messageDetails.XML_MsgConfirmation.toString().equals(XMLchars.nanXSLT_Result) ) {
                messageDetails.MsgReason.append("В результате XSLT преобразования получен пустой XML для заголовка сообщения");
                throw new TransformerException( messageDetails.MsgReason.toString()) ;
            }
            // Всё ок, записываем в  XML_MsgClear результат из  XML_MsgConfirmation
            messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();
            messageDetails.XML_MsgClear.append( messageDetails.XML_MsgConfirmation.toString());
            // очищаем использованный XML_MsgConfirmation
            messageDetails.XML_MsgConfirmation.setLength(0); messageDetails.XML_MsgConfirmation.trimToSize();
        }

        // 2й проход - получаем элемент Context из заголовка ( если есть )
        InputStream parsedXML_MsgClearStream = new ByteArrayInputStream(messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8));
        try {
            messageDetails.Input_Clear_XMLDocument  =  documentBuilder.build(parsedXML_MsgClearStream); // .parse(parsedConfigStream);
        }
        catch ( JDOMParseException e)
        {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgClear.toString() + ")fault"  );
            throw new JDOMParseException("client.post:getClearRequest=(" + messageDetails.XML_MsgClear.toString() + ")", e);
        }

        SoapEnvelope = messageDetails.Input_Clear_XMLDocument.getRootElement();
        boolean isSoapBodyFinded = false;
        List<Element> SoapEnvelopeList = SoapEnvelope.getChildren();
        // Перебор всех элементов Envelope
        for (int i = 0; i < SoapEnvelopeList.size(); i++) {
            Element SoapElmnt = SoapEnvelopeList.get(i);
            MessegeSend_Log.warn(" makeClearRequest:.getRootElement: SoapEnvelopeList.get("+ i+ ") SoapElmnt.getName()= (" + SoapElmnt.getName() + ")"  );
            if ( SoapElmnt.getName().equals(XMLchars.Body) ) {
                List<Element> Request_MethodList = SoapElmnt.getChildren();
                if ( Request_MethodList.size() == 1 ) {
                    Element Request_Method = Request_MethodList.get(0);
                    messageDetails.Request_Method = Request_Method;

                    // Формируем XML-тест содержимого Body включая элемент-метод
                    messageDetails.XML_Request_Method.setLength(0); messageDetails.XML_Request_Method.trimToSize();
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + Request_Method.getName() + XMLchars.CloseTag);
                        XML_RequestElemets2StringB( messageDetails, Request_Method, MessegeSend_Log);
                    messageDetails.XML_Request_Method.append(XMLchars.OpenTag + XMLchars.EndTag + Request_Method.getName() + XMLchars.CloseTag);

                }
                else {
                    MessegeSend_Log.error("makeClearRequest: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size() + " в(" + messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8) + ")"  );
                    throw new XPathExpressionException("makeClearRequest: в SOAP-запросе внутри <Body> количество элементов=" + Request_MethodList.size());
                }
                isSoapBodyFinded = true;

            }
            if ( SoapElmnt.getName().equals(XMLchars.Header) ) {
                Element hContext = SoapElmnt.getChild("Context");
                if ( hContext != null) {
                    messageDetails.Input_Header_Context = hContext;
                   // MessegeSend_Log.info("makeClearRequest: в SOAP-запросе внутри <Header> нашли элемент=Context в(" + messageDetails.XML_MsgClear.toString() + ")"  );
                }
                else {
                    messageDetails.Input_Header_Context = null;
                   // MessegeSend_Log.warn("makeClearRequest: в SOAP-запросе внутри <Header> НЕ НАШЛИ элемент=Context в(" + messageDetails.XML_MsgClear.toString() + ")"  );
                }

            }

        }

        if ( !isSoapBodyFinded ) {
            MessegeSend_Log.error("documentBuilder.build (" + messageDetails.XML_MsgClear.toString().getBytes(StandardCharsets.UTF_8) + ")fault"  );
            throw new XPathExpressionException("makeClearRequest: в SOAP-запросе не найден Element=" + XMLchars.Body);
        }

        XMLOutputter xmlOutputter = new XMLOutputter();
        xmlOutputter.setFormat(Format.getPrettyFormat());
        messageDetails.XML_MsgClear.setLength(0); messageDetails.XML_MsgClear.trimToSize();
        messageDetails.XML_MsgClear.append(xmlOutputter.outputString(messageDetails.Input_Clear_XMLDocument));
        return messageDetails.XML_MsgClear.toString();
    }

    public static int SoapBody2XML_String(@NotNull MessageDetails messageDetails, Element SoapEnvelope, Logger MessegeSend_Log) {

        int BodyListSize = 0;

            List<Element> list = SoapEnvelope.getChildren();
            // Перебор всех элементов Envelope
            for (int i = 0; i < list.size(); i++) {
                Element SoapElmnt = (Element) list.get(i);
                //MessegeSend_Log.info("client.post:SoapBody2XML_String=(" + SoapElmnt.getName() + " =" + SoapElmnt.getText() + ")");
                // надо подготовить очищенный от ns: содержимое Body.
                messageDetails.XML_MsgClear.append(XMLchars.OpenTag + SoapElmnt.getName() + XMLchars.CloseTag);
                XMLutils.XML_BodyElemets2StringB(messageDetails, SoapElmnt, MessegeSend_Log);
                messageDetails.XML_MsgClear.append(XMLchars.OpenTag + XMLchars.EndTag + SoapElmnt.getName() + XMLchars.CloseTag);
                // MessegeSend_Log.info("SoapBody2XML_String(XML_ClearBodyResponse):" + messageDetails.XML_MsgClear.toString());
            }

        return BodyListSize;

    }

    public static int XML_RequestElemets2StringB(MessageDetails messageDetails, Element EntryElement,
                                              Logger MessegeSend_Log) {
        int nn = 0;
        //MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);
            String ElementEntry = XMLelement.getName();

            String ElementContent = XMLelement.getTextTrim() ; // XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"

            messageDetails.XML_Request_Method.append(XMLchars.OpenTag + ElementEntry);
            //MessegeSend_Log.info("XML_MsgClear.appendBEGIB(" + XMLchars.OpenTag + ElementEntry + ")");
            //MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");
            //MessegeSend_Log.info("XML_Body-XMLelement.getText {<" + XMLelement.getText() + ">}" + " <ElementContent>" + ElementContent + " </ElementContent>" );

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                Attribute XMLattribute = ElementAttributes.get(j);

                String AttributeEntry = XMLattribute.getName();
                String AttributeValue = XMLattribute.getValue();  //XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());

                messageDetails.XML_Request_Method.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
                //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
            }
            messageDetails.XML_Request_Method.append(XMLchars.CloseTag);

            //MessegeSend_Log.info("XML_BodyElemets2StringB ElementContent.length =" + ElementContent.length() + ";");
            if ( ElementContent.length() > 0 ) {
                messageDetails.XML_Request_Method.append(ElementContent);
                //MessegeSend_Log.info("XML_BodyElemets2StringB-ElementContent[" + ElementContent + "]");
            }


            XML_RequestElemets2StringB(messageDetails, XMLelement,
                    MessegeSend_Log);
            messageDetails.XML_Request_Method.append(XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag);
            //MessegeSend_Log.info("XML_MsgClear.appendEND(" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + ")" );
            //MessegeSend_Log.info("XML_MsgClear.length=" +  messageDetails.XML_MsgClear.length() );
            //MessegeSend_Log.info("XML_MsgClear.String=" +  messageDetails.XML_MsgClear.toString() );

            //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");

        }
        return nn;

    }

    public static int XML_BodyElemets2StringB(MessageDetails messageDetails, Element EntryElement,
                                              Logger MessegeSend_Log) {

        int nn = 0;
         //MessegeSend_Log.info("XML_BodyElemets2StringB: <" + EntryElement.getName() + ">");
        List<Element> Elements = EntryElement.getChildren();
        // Перебор всех элементов TemplConfig
        for (int i = 0; i < Elements.size(); i++) {
            Element XMLelement = (Element) Elements.get(i);
            String ElementEntry = XMLelement.getName();

            String ElementContent = XMLelement.getTextTrim() ; // XmlEscapers.xmlAttributeEscaper().escape( XMLelement.getText()); //.getText(); заменил на getValue() из-за "<>"

            messageDetails.XML_MsgClear.append(XMLchars.OpenTag + ElementEntry);
            //MessegeSend_Log.info("XML_MsgClear.appendBEGIB(" + XMLchars.OpenTag + ElementEntry + ")");
            //MessegeSend_Log.info("XML_BodyElemets2StringB {<" + ElementEntry + ">}");
            //MessegeSend_Log.info("XML_Body-XMLelement.getText {<" + XMLelement.getText() + ">}" + " <ElementContent>" + ElementContent + " </ElementContent>" );

            List<Attribute> ElementAttributes = XMLelement.getAttributes();
            for (int j = 0; j < ElementAttributes.size(); j++) {
                Attribute XMLattribute = ElementAttributes.get(j);

                String AttributeEntry = XMLattribute.getName();
                String AttributeValue = XMLattribute.getValue();  //XmlEscapers.xmlAttributeEscaper().escape( XMLattribute.getValue());

                messageDetails.XML_MsgClear.append(XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote);
                //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.Space + AttributeEntry + XMLchars.Equal + XMLchars.Quote + AttributeValue + XMLchars.Quote + "}");
            }
            messageDetails.XML_MsgClear.append(XMLchars.CloseTag);

            //MessegeSend_Log.info("XML_BodyElemets2StringB ElementContent.length =" + ElementContent.length() + ";");
            if ( ElementContent.length() > 0 ) {
                messageDetails.XML_MsgClear.append(ElementContent);
                //MessegeSend_Log.info("XML_BodyElemets2StringB-ElementContent[" + ElementContent + "]");
            }


            XML_BodyElemets2StringB(messageDetails, XMLelement,
                    MessegeSend_Log);
            messageDetails.XML_MsgClear.append(XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag);
            //MessegeSend_Log.info("XML_MsgClear.appendEND(" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + ")" );
            //MessegeSend_Log.info("XML_MsgClear.length=" +  messageDetails.XML_MsgClear.length() );
            //MessegeSend_Log.info("XML_MsgClear.String=" +  messageDetails.XML_MsgClear.toString() );

            //MessegeSend_Log.info("XML_BodyElemets2StringB{" + XMLchars.OpenTag + XMLchars.EndTag + ElementEntry + XMLchars.CloseTag + "}");

        }
        return nn;
    }



    public static String ConvXMLuseXSLT(@NotNull Long QueueId, @NotNull String XMLdata_4_Tranform, @NotNull String XSLTdata, StringBuilder MsgResult, StringBuilder ConvXMLuseXSLTerr,
                                        xlstErrorListener XSLTErrorListener, Logger MessegeSend_Log, boolean IsDebugged )
            throws TransformerException
    { StreamSource source,srcxslt;
        Transformer transformer;
        StreamResult result;
        ByteArrayInputStream xmlInputStream=null;
        ByteArrayOutputStream fout=new ByteArrayOutputStream();
        String res=XMLchars.EmptyXSLT_Result;
        ConvXMLuseXSLTerr.setLength(0); ConvXMLuseXSLTerr.trimToSize();
        try {
            xmlInputStream  = new ByteArrayInputStream(XMLdata_4_Tranform.getBytes("UTF-8"));

        }
        catch ( Exception exp ) {
            ConvXMLuseXSLTerr.append(  sStackTracе.strInterruptedException(exp) );
            exp.printStackTrace();
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.ByteArrayInputStream Exception" );
            MessegeSend_Log.error("["+ QueueId  + "] Exception: " + ConvXMLuseXSLTerr );
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT:"  + ConvXMLuseXSLTerr );
            return XMLchars.EmptyXSLT_Result ;
        }

        source = new StreamSource(xmlInputStream);
        srcxslt = new StreamSource(new ByteArrayInputStream(XSLTdata.getBytes()));
        result = new StreamResult(fout);
        try
        {
            TransformerFactory XSLTransformerFactory = TransformerFactory.newInstance();
            XSLTransformerFactory.setErrorListener( XSLTErrorListener ); //!!!! java.lang.IllegalArgumentException: ErrorListener !!!
           /* XSLTransformerFactory.setErrorListener(new ErrorListener() {
                public void warning(TransformerException te) {
                    log.warn("Warning received while processing a stylesheet", te);
                }
                */
            // transformer = TransformerFactory.newInstance().newTransformer(srcxslt);
            transformer = XSLTransformerFactory.newTransformer(srcxslt);
            if ( transformer != null) {
                transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
                transformer.transform(source, result);
            }
            else result = null;

            if ( result != null) {
                res = fout.toString();
                // System.err.println("result != null, res:" + res );
                if ( res.length() < XMLchars.EmptyXSLT_Result.length())
                    res = XMLchars.EmptyXSLT_Result;
            }
            else {
                res = XMLchars.EmptyXSLT_Result;
                // System.err.println("result= null, res:" + res );
            }
            try { fout.close();} catch( IOException IOexc)  { ; }
            if ( IsDebugged ) {
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.info("["+ QueueId  + "] ConvXMLuseXSLT( XML out ): " + res);
            }
        }
        catch ( TransformerException exp ) {
            ConvXMLuseXSLTerr.append(  sStackTracе.strInterruptedException(exp) );
            System.err.println( "["+ QueueId  + "] ConvXMLuseXSLT.Transformer Exception" );
            exp.printStackTrace();

            if (  !IsDebugged ) {
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML IN ): " + XMLdata_4_Tranform);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XSLT ): " + XSLTdata);
                MessegeSend_Log.error("["+ QueueId  + "]ConvXMLuseXSLT( XML out ): " + res);
            }
            MessegeSend_Log.error("["+ QueueId  + "] Transformer.Exception: " + ConvXMLuseXSLTerr);
            MsgResult.setLength(0);
            MsgResult.append( "ConvXMLuseXSLT.Transformer:"  + ConvXMLuseXSLTerr );
            throw exp;
            // return XMLchars.EmptyXSLT_Result ;
        }
        return(res);
    }

    public static  boolean TestXMLByXSD(@NotNull long Queue_Id, @NotNull String XMLdata_4_Validate, @NotNull String xsddata, StringBuilder MsgResult,  Logger MessegeSend_Log)// throws Exception
    {
        Validator valid=null;
        StreamSource reqwsdl=null, xsdss = null;
        Schema shm= null;

        try
        { reqwsdl = new StreamSource(new ByteArrayInputStream(XMLdata_4_Validate.getBytes()));
            xsdss = new StreamSource(new ByteArrayInputStream(xsddata.getBytes()));
            shm = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(xsdss);
            valid =shm.newValidator();
            valid.validate(reqwsdl);

        }
        catch ( Exception exp ) {
            MessegeSend_Log.error("Exception: " + exp.getMessage());
            MsgResult.setLength(0);
            MsgResult.append( "["+ Queue_Id  + "]  TestXMLByXSD:"  + sStackTracе.strInterruptedException(exp) );
            return false;}
        //MessegeSend_Log.info("validateXMLSchema message\n" + XMLdata_4_Validate + "\n is VALID for XSD\n" + xsddata );
        return true;
    }


}
