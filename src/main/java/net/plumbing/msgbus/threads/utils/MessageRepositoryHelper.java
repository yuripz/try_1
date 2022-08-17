package net.plumbing.msgbus.threads.utils;

import net.plumbing.msgbus.model.*;
import org.slf4j.Logger;

public class MessageRepositoryHelper {

    public static  int look4MessageDirectionsVO_2_MsgDirection_Cod( String MsgDirection_Cod, Logger messegeSend_log) {
        int MsgDirectionVO_Key=-1;
        int MsgDirectionVO_4_Direction_Key=-1;

        for (int j = 0; j < MessageDirections.AllMessageDirections.size(); j++) {
            MessageDirectionsVO messageDirectionsVO = MessageDirections.AllMessageDirections.get(j);
            if ( messageDirectionsVO.getMsgDirection_Cod().equalsIgnoreCase( MsgDirection_Cod ))
                MsgDirectionVO_4_Direction_Key = j;
        }
        if (MsgDirectionVO_4_Direction_Key > 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_Key;
        return  MsgDirectionVO_Key;
    }

    public static  int look4MessageDirectionsVO_2_Perform(int MessageMsgDirection_id, String MessageSubSys_cod, Logger messegeSend_log) {
        int MsgDirectionVO_Key=-1;
        int MsgDirectionVO_4_Direction_Key=-1;
        int MsgDirectionVO_4_Direction_SubSys_Id=-1;

        for (int j = 0; j < MessageDirections.AllMessageDirections.size(); j++) {
            MessageDirectionsVO messageDirectionsVO = MessageDirections.AllMessageDirections.get(j);
            if ( messageDirectionsVO.getMsgDirection_Id() == MessageMsgDirection_id )
            { String DirectionsSubSys_Cod = messageDirectionsVO.getSubsys_Cod();
                if ( (DirectionsSubSys_Cod == null) || (DirectionsSubSys_Cod).equals("0") ) {
                    //  заполнен код ПодСистемы : MESSAGE_DIRECTIONS.subsys_cod == '0' OR MESSAGE_DIRECTIONS.subsys_cod is NULL )
                    MsgDirectionVO_4_Direction_Key = j;
                }
                else {
                    if ( DirectionsSubSys_Cod.equals( MessageSubSys_cod ))
                        MsgDirectionVO_4_Direction_SubSys_Id = j;
                }

            }
        }
        if (MsgDirectionVO_4_Direction_Key > 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_Key;
        if (MsgDirectionVO_4_Direction_SubSys_Id > 0 ) MsgDirectionVO_Key = MsgDirectionVO_4_Direction_SubSys_Id;
        return  MsgDirectionVO_Key;
    }

    public static  int look4MessageTypeVO_2_Perform(int Operation_Id,  Logger messegeSend_log) {
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if ( messageTypeVO.getOperation_Id() == Operation_Id ) {    //  нашли операцию,
                return  i;
            }
        }
        return -1;
    }

    public static  int look4MessageTypeVO_2_Interface(String Url_Soap_Send,  Logger messegeSend_log) {
        messegeSend_log.info("look4MessageTypeVO_2_Interface[0-" + MessageType.AllMessageType.size() + "]:" + Url_Soap_Send);
        for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
            MessageTypeVO messageTypeVO = MessageType.AllMessageType.get(i);
            if (messageTypeVO.getOperation_Id() == 0 ){ // Это ИНТПРФЕЙС, тип, у которого № ОПЕРАЦИЯ == 0
                String URL_SOAP_Send = messageTypeVO.getURL_SOAP_Send();
                if ( URL_SOAP_Send != null ) {
                    if ( URL_SOAP_Send.equals(Url_Soap_Send) ) {    //  нашли операцию,
                        return messageTypeVO.getInterface_Id(); // i;
                    }
                }
            }
        }
        return -1;
    }

    public static  int look4MessageTemplate_2_Interface(int look4_Interface_Id,  Logger messegeSend_log) {
        messegeSend_log.info("look4MessageTemplate_2_Interface[" + MessageTemplate.AllMessageTemplate.size() + "]:" + look4_Interface_Id);
        int MessageTemplateVOkey=-1;

        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            int Operation_Id = messageTemplateVO.getOperation_Id();
            int Interface_Id = messageTemplateVO.getInterface_Id();
            // messegeSend_log.info("look4MessageTemplate, проверяем MessageTemplateVOkey=[" + MessageTemplateVOkey +"]: Operation_Id =" + Operation_Id + ", Interface_Id =" + Interface_Id );

            if ((Interface_Id == look4_Interface_Id) && ( Operation_Id== 0)) {
                // №№ Шаблонов совпали,  Template_Id = i;
                MessageTemplateVOkey = i;
                messegeSend_log.info( "look4MessageTemplate_2_Interface: используем [" + MessageTemplateVOkey +"]: Template_Id=" +
                        MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_Id() +
                        ", Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_name() );
                return MessageTemplateVOkey;
            }
        }
        messegeSend_log.info("look4MessageTemplate, получаем MessageTemplateVOkey=[" + MessageTemplateVOkey +"]: значит, не нашли");

        return MessageTemplateVOkey;
    }

    public static int look4MessageTemplate( int look4Template_Id,
                                            Logger messegeSend_log) {
        int MessageTemplateVOkey=-1;

        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            int Template_Id = messageTemplateVO.getTemplate_Id();

            if (Template_Id == look4Template_Id) {
                // №№ Шаблонов совпали,  Template_Id = i;
                MessageTemplateVOkey = i;
                messegeSend_log.info( "look4MessageTemplate: используем [" + MessageTemplateVOkey +"]: Template_Id=" +
                        MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_Id() +
                        ", Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplateVOkey).getTemplate_name()
                );
                return MessageTemplateVOkey;
            }
        }
        messegeSend_log.info("look4MessageTemplate, получаем MessageTemplateVOkey=[" + MessageTemplateVOkey +"]: значит, не нашли");

        return MessageTemplateVOkey;
    }

    public static int look4MessageTemplateVO_2_Perform( int Operation_Id,
                                                  int MsgDirection_Id,
                                                  String  SubSys_Cod  , Logger MessegeSend_Log) {
        int Template_Id=-1;
        int Template_All_Id=-1;
        int Template_4_Direction_Id=-1;
        int Template_4_Direction_SubSys_Id=-1;

        int Type_Id = -1;
        int TemplateOperation_Id ;
        int TemplateMsgDirection_Id;
        String  TemplateSubSys_Cod;
        // 1) пробегаем по Типам сообщений
        for (int i = 0; i < MessageType.AllMessageType.size(); i++)
        {
            MessageTypeVO  messageTypeVO = MessageType.AllMessageType.get(i);
            if ( messageTypeVO.getOperation_Id() == Operation_Id )
            {
                //  нашли операцию,
                Type_Id = i;
            }
        }

        if ( Type_Id < 0) {
            MessegeSend_Log.info("Operation[" + Operation_Id + "] is not found in any MessageType");
            return Template_Id;
        }
        for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
            MessageTemplateVO messageTemplateVO = MessageTemplate.AllMessageTemplate.get( i );
            TemplateOperation_Id = messageTemplateVO.getOperation_Id();
            TemplateMsgDirection_Id = messageTemplateVO.getSource_Id();
            TemplateSubSys_Cod = messageTemplateVO.getSrc_SubCod();

            // MessegeSend_Log.info("[" + i + "] № операции (" + TemplateOperation_Id + ") TemplateMsgDirection_Id =[" + TemplateMsgDirection_Id + "], TemplateSubSys_Cod=" + TemplateSubSys_Cod );

            if (TemplateOperation_Id == Operation_Id) {
                // № операции совпали,  Template_Id = i;
                //  MessegeSend_Log.info("[" + i + "] № операции (" + Operation_Id + ") совпали =[" + TemplateOperation_Id + "], " + messageTemplateVO.getTemplate_name() );
                //  MessegeSend_Log.info("[" + i + "] Template_Id (" + messageTemplateVO.getTemplate_Id() + ") смотрим TemplateSubSys_Cod =[" + TemplateSubSys_Cod + "]" );

                if ( (TemplateSubSys_Cod == null) || (TemplateSubSys_Cod).equals("0") || (TemplateSubSys_Cod).equals("") )
                { // в Шаблоне не заполнен код ПодСистемы : MESSAGE_templateS.dst_subcod == '0' OR MESSAGE_templateS.dst_subcod is NULL )
                    // сравниваем по коду сисмемы Шаблона MESSAGE_templateS.destin_id и сообщения MESSAGE_QUEUE.MsgDirection_Id
                    //     MessegeSend_Log.info("сравниваем по коду сисмемы Шаблона MESSAGE_templateS.destin_id " + TemplateMsgDirection_Id + " и сообщения MESSAGE_QUEUE.MsgDirection_Id");

                    if (( TemplateMsgDirection_Id != 0 ) && (TemplateMsgDirection_Id == MsgDirection_Id )){
                        // совпали Идентификаторы систем
                        Template_4_Direction_Id= i;
                        //       MessegeSend_Log.info("Идентификаторы систем (" + MsgDirection_Id + ") совпали[" + TemplateMsgDirection_Id + "]=" + messageTemplateVO.getTemplate_name() );
                    }
                    if ( ( TemplateMsgDirection_Id == 0 )) {
                        // Шаблон для любой системы
                        Template_All_Id = i;
                        //     MessegeSend_Log.info("Шаблон для любой системы(" + messageTemplateVO.getDestin_Id() + ") совпали[" + messageTemplateVO.getTemplate_Id() + "]=" + messageTemplateVO.getTemplate_name() );
                    }

                }
                else { // в Шаблоне Заполнен код ПодСистемы : MESSAGE_templateS.dst_subcod is NOT null -> MESSAGE_templateS.destin_id is NOT null too !
                    // проверяем на полное совпадение
                    //    MessegeSend_Log.info("сравниваем по коду ПОДсистемы Шаблона "+ TemplateSubSys_Cod + " и MESSAGE_templateS.destin_id " + TemplateMsgDirection_Id +
                    //            " с сообщением MESSAGE_QUEUE.SubSys_Cod(" + SubSys_Cod + ") MESSAGE_QUEUE.MsgDirection_Id(" + MsgDirection_Id + ")");
                    if ( (TemplateSubSys_Cod.equals(SubSys_Cod) ) && (TemplateMsgDirection_Id == MsgDirection_Id ) ) {
                        Template_4_Direction_SubSys_Id = i;
                        //     MessegeSend_Log.info("Идентификаторы систем (" + MsgDirection_Id + ") совпали[" + messageTemplateVO.getTemplate_Id() + "]=" + " коды подСистем тоже совпали (" + SubSys_Cod + ") " + messageTemplateVO.getTemplate_name());
                    }
                }

            }
        }
        // уточняем точность находки в порядке широты применения
        if ( Template_All_Id > 0 ) Template_Id = Template_All_Id;
        if ( Template_4_Direction_Id > 0 ) Template_Id = Template_4_Direction_Id;
        if ( Template_4_Direction_SubSys_Id > 0 ) Template_Id = Template_4_Direction_SubSys_Id;
        if ( Template_Id >= 0 )
            MessegeSend_Log.info("Итого, используем [" + Template_Id +"]: Template_Id=" + MessageTemplate.AllMessageTemplate.get(Template_Id).getTemplate_Id());
        else
            MessegeSend_Log.error("Итого, получаем Template_Id=[" + Template_Id +"]: значит, не нашли");

        return Template_Id;

    }
}
