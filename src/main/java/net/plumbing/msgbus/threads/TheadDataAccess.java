package net.plumbing.msgbus.threads;

import com.zaxxer.hikari.HikariDataSource;
// import oracle.jdbc.internal.PreparedStatement;
// import oracle.jdbc.internal.OracleTypes;
// import oracle.jdbc.internal.OracleRowId;
// import oracle.sql.NUMBER;
import org.slf4j.Logger;
import net.plumbing.msgbus.common.XMLchars;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.sql.*;


public class TheadDataAccess {
    private final int maxReasonLen =1996;
    public  Connection  Hermes_Connection=null;
    public PreparedStatement stmtMsgQueue=null;
    public PreparedStatement stmtMsgQueueConfirmationDet=null;
    public PreparedStatement stmtMsgQueueConfirmationTag = null;
    public PreparedStatement stmtMsgQueueBody = null;
    public PreparedStatement stmtMsgLastBodyTag = null;
    public PreparedStatement stmtMsgQueueConfirmation = null;
    public PreparedStatement stmt_UPDATE_MessageQueue_In2Ok = null;
    public PreparedStatement stmt_UPDATE_MessageQueue_Temp2ErrIN= null;

    /*
    public PreparedStatement stmtUPDATE_MessageQueue_Queue_Date4Send;
    // HE-5481  q.Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    public final String UPDATE_MessageQueue_Queue_Date4Send =
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Date = sysdate, q.Queue_Direction = 'SEND'" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count=1 " +
                    ", q.Prev_Queue_Direction='OUT', q.Prev_Msg_Date=sysdate " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmtUPDATE_MessageQueue_Out2Send;
    // HE-5481  q.Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    public final String UPDATE_MessageQueue_Out2Send =
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
            "set q.Queue_Date = sysdate, q.Queue_Direction = 'SEND', q.Msg_Reason = ?" +
            ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count=1 " +
            ", q.Prev_Queue_Direction='OUT', q.Prev_Msg_Date=sysdate " +
            "where 1=1 and q.Queue_Id = ?  ";
    */
    public PreparedStatement stmtUPDATE_MessageQueue_ExeIn2DelIN=null;
    public final String UPDATE_MessageQueue_ExeIn2DelIN=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'DELIN'" +
                    ", q.Msg_Date= sysdate" +
                    ", q.Prev_Queue_Direction= q.Queue_Direction, q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmtUPDATE_MessageQueue_In2ErrorIN=null;
    public final String UPDATE_MessageQueue_In2ErrorIN=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'ERRIN', q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                    ", q.Prev_Queue_Direction= q.Queue_Direction, q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmtUPDATE_MessageQueue_Out2ErrorOUT;
    public final String UPDATE_MessageQueue_Out2ErrorOUT=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'ERROUT', q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = 1030, q.Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                    ", q.Prev_Queue_Direction='OUT', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";


    public PreparedStatement stmtUPDATE_MessageQueue_Send2ErrorOUT;
    public final String UPDATE_MessageQueue_Send2ErrorOUT=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'ERROUT', q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";
/*
    public final String UPDATE_MessageQueue_SetMsg_Reason=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'RESOUT', q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ? ";
    public PreparedStatement stmtUPDATE_MessageQueue_SetMsg_Reason;

    public PreparedStatement stmtUPDATE_MessageQueue_Send2AttOUT;
    public final String UPDATE_MessageQueue_Send2AttOUT=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = 'ATTOUT', q.Msg_Result = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";

    public PreparedStatement stmt_UPDATE_MessageQueue_Send2finishedOUT;
    public final String UPDATE_MessageQueue_Send2finishedOUT=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Queue_Direction = ?, q.Msg_Reason = ?" +
                    ", q.Msg_Date= sysdate,  q.Msg_Status = ?, q.Retry_Count= ? " +
                    ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?  ";
*/
    public PreparedStatement stmt_UPDATE_MessageQueue_DirectionAsIS;
    public final String UPDATE_MessageQueue_DirectionAsIS=
            "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                    "set q.Msg_Date= sysdate + (?/(24*3600)), q.Msg_Reason = ?, q.Msg_Status = ?, q.Retry_Count= ?, " +
                    " q.Prev_Msg_Date=q.Msg_Date " +
                    "where 1=1 and q.Queue_Id = ?";


    public PreparedStatement stmt_UPDATE_MessageQueue_after_FaultResponse;
    public PreparedStatement stmt_UPDATE_after_FaultGet;

    public final String UPDATE_QUEUElog_Response="update ARTX_PROJ.MESSAGE_QUEUElog L set l.Resp_DT = sysdate, l.Response = ? where l.QUEUE_ID= ? and l.ROWID = ?";
    public PreparedStatement stmt_UPDATE_QUEUElog;
    public final String INSERT_QUEUElog_Request="{call insert into ARTX_PROJ.MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, sysdate, ?) returning ROWID into ? }";
    public CallableStatement stmt_INSERT_QUEUElog;

    public PreparedStatement stmt_DELETE_Message_Details;
    public final String DELETE_Message_Details= "delete from ARTX_PROJ.MESSAGE_QueueDET D where D.queue_id =?";

    public PreparedStatement stmt_DELETE_Message_Confirmation;
    public final String DELETE_Message_Confirmation= "delete from ARTX_PROJ.MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_par_num >=" +
            "(select min(d.tag_num) from ARTX_PROJ.MESSAGE_QUEUEDET d where d.queue_id = ? and d.tag_id='Confirmation')";
    public PreparedStatement stmt_DELETE_Message_ConfirmationH;
    public final String DELETE_Message_ConfirmationH= "delete from ARTX_PROJ.MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_id='Confirmation'";

    public PreparedStatement stmt_Query_Message_Confirmation;
    public final String SELECT_Message_Confirmation= "select from ARTX_PROJ.MESSAGE_QueueDET D where D.queue_id =? and d.Tag_num >= ?";


    public PreparedStatement stmt_INSERT_Message_Details;
    public final String INSERT_Message_Details= "INSERT into ARTX_PROJ.MESSAGE_QueueDET (QUEUE_ID, TAG_ID, TAG_VALUE, TAG_NUM, TAG_PAR_NUM) " +
                                                                                "values (?, ?, ?, ?, ?)";
    public final String selectMessageStatement =
            " select ARTX_PROJ.MESSAGE_QUEUE_SEQ.NEXTVAL as queue_id," +
                    " '" +XMLchars.DirectTEMP +"' as queue_direction," +
                    " (sysdate - to_date('01.01.1970','DD.MM.YYYY')) * (24*60*60) Queue_Date," +
                    " 0 msg_status," +
                    " (sysdate - to_date('01.01.1970','DD.MM.YYYY')) * (24*60*60) Msg_Date," +
                    " 0 operation_id," +
                    " 0 outqueue_id," +
                    " 'Undefine' as msg_type," +
                    " NULL as  msg_reason," +
                    " 0 msgdirection_id," +
                    " 0 msg_infostreamid," +
                    " NULL msg_type_own," +
                    " NULL msg_result," +
                    " NULL subsys_cod," +
                    " 0 as Retry_Count," +
                    " NULL prev_queue_direction," +
                    " (sysdate - to_date('01.01.1970','DD.MM.YYYY')) * (24*60*60) Prev_Msg_Date, " +
                    " (sysdate - to_date('01.01.1970','DD.MM.YYYY')) * (24*60*60) Queue_Create_Date " +
                    "from DUAL "  ;
    public PreparedStatement stmt_New_Queue_Prepare;
    public final String INSERT_Message_Queue= "INSERT into ARTX_PROJ.MESSAGE_Queue " +
                   "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) " +
            "values (?,        'TEMP',          sysdate,    0,          sysdate,  0,            0,          'Undefine')";
    public PreparedStatement stmt_New_Queue_Insert;


    public void close_Hermes_Connection() {
        try { this.Hermes_Connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public   Connection make_Hikari_Connection(  String db_userid , String db_password,
                                                 HikariDataSource dataSource,
                                                 Logger dataAccess_log) {
        Connection Target_Connection = null;
        String connectionUrl= dataSource.getJdbcUrl() ;
        // попробуй ARTX_PROJ / rIYmcN38St5P  || hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        dataAccess_log.info( "Try(thead) Hermes getConnection: " + connectionUrl + " as " + db_userid );

        try {
        Hermes_Connection = dataSource.getConnection();
            Hermes_Connection.setAutoCommit(false);
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );
        Target_Connection = Hermes_Connection;

        if (  make_Message_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Query() fault");
            return null;
        }

        if (  make_Message_QueryConfirmation(dataAccess_log) == null ) {
               dataAccess_log.error( "make_Message_QueryConfirmation() fault");
               return null;
           }
        if (  make_Message_LastBodyTag_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_LastBodyTag_Query() fault");
            return null;
        }
        if (  make_Message_ConfirmationTag_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_ConfirmationTag_Query() fault");
            return null;
        }
        if (  make_MessageBody_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_MessageBody_Query() fault");
            return null;
        }

        if (  make_MessageConfirmation_Query(dataAccess_log) == null ) {
            dataAccess_log.error( "make_MessageConfirmation_Query() fault");
            return null;
        }
/*
        if (  make_Message_Update_Out2Send(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_Out2Send() fault");
            return null;
        }
        if (  make_Message_Update_Out2ErrorOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_Out2ErrorOUT() fault");
            return null;
        }
*/
        if (  make_delete_Message_Details(dataAccess_log) == null ) {
            dataAccess_log.error( "make_delete_Message_Details() fault");
            return null;
        }

        if (  make_insert_Message_Details(dataAccess_log) == null ) {
            dataAccess_log.error( "make_insert_Message_Details() fault");
            return null;
        }

        if ( make_Message_Update_Send2ErrorOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_Send2ErrorOUT() fault");
            return null;
        }
/*
        if ( make_UPDATE_MessageQueue_Send2finishedOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }
        */

        /*
        if ( make_UPDATE_MessageQueue_Send2AttOUT(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }
        */
        if ( make_DELETE_Message_Confirmation(dataAccess_log) == null ) {
            dataAccess_log.error( "make_DELETE_Message_Confirmation() fault");
            return null;
        }



        if ( make_UPDATE_MessageQueue_DirectionAsIS(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_DirectionAsIS() fault");
            return null;
        }

        if ( make_UPDATE_QUEUElog(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_QUEUElog() fault");
            return null;
        }

        if ( make_INSERT_QUEUElog(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_QUEUElog() fault");
            return null;
        }

        /*if ( make_UPDATE_MessageQueue_SetMsg_Reason(dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }*/
        /*
        if ( make_Message_Update_Queue_Queue_Date4Send( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }
        */
        if (make_SelectNew_Queue( dataAccess_log) == null ) {
            dataAccess_log.error( "make_SelectNew_Queue() fault");
            return null;
        }
        if (make_insert_Message_Queue( dataAccess_log) == null ) {
            dataAccess_log.error( "make_insert_Message_Queue() fault");
            return null;
        }

        if ( make_Message_Update_In2ErrorIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_In2ErrorIN() fault");
            return null;
        }
        if ( make_UPDATE_MessageQueue_In2Ok( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_In2Ok() fault");
            return null;
        }
        if ( make_UPDATE_MessageQueue_Temp2ErrIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_Temp2ErrIN() fault");
            return null;
        }
        if ( make_Message_Update_ExeIn2DelIN( dataAccess_log) == null ) {
            dataAccess_log.error( "make_Message_Update_ExeIn2DelIN() fault");
            return null;
        }

        return Target_Connection;
    }

    public PreparedStatement make_SelectNew_Queue( Logger dataAccess_log )
    {
        PreparedStatement StmtMsg_Queue;
        try {

        dataAccess_log.info( "MESSAGE_QueueSelect4insert:" + selectMessageStatement );
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( selectMessageStatement);
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }

        this.stmt_New_Queue_Prepare = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_insert_Message_Queue( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Queue );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_New_Queue_Insert = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }


    public PreparedStatement  make_UPDATE_QUEUElog( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( UPDATE_QUEUElog_Response );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }

        this.stmt_UPDATE_QUEUElog = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
    //public final String UPDATE_QUEUElog_Response="update ARTX_PROJ.MESSAGE_QUEUElog L set l.Resp_DT = sysdate, l.Response = ? where l.Queue_Id = ?";
    //public PreparedStatement stmt_UPDATE_QUEUElog;

    public  int doUPDATE_QUEUElog(@NotNull RowId ROWID_QUEUElog, @NotNull long Queue_Id, String sResponse,
                                                       Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_QUEUElog: \"update ARTX_PROJ.MESSAGE_QUEUElog L set l.Resp_DT = sysdate, l.Response = '"+sResponse+ "' " +
                "where l.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_QUEUElog.setRowId( 3,ROWID_QUEUElog);
            stmt_UPDATE_QUEUElog.setLong( 2, Queue_Id );
            stmt_UPDATE_QUEUElog.setString( 1, sResponse );
            stmt_UPDATE_QUEUElog.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + UPDATE_QUEUElog_Response + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public CallableStatement  make_INSERT_QUEUElog( Logger dataAccess_log ) {
        CallableStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareCall( INSERT_QUEUElog_Request );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (CallableStatement) null );
        }
        //this.stmt_INSERT_QUEUElog = (OraclePreparedStatement)StmtMsg_Queue;
        this.stmt_INSERT_QUEUElog = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }
    //public final String INSERT_QUEUElog_Request="insert into ARTX_PROJ.MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, sysdate, ?)";
    // public PreparedStatement stmt_INSERT_QUEUElog;
    public  RowId doINSERT_QUEUElog(@NotNull long Queue_Id, String sRequest,
                                  Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] do {call insert into ARTX_PROJ.MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values(" + Queue_Id + ", sysdate, '"+sRequest+ "' ) returning ROWID into ? };" );
        int count ;
        RowId ROWID_QUEUElog=null;
        try {
            stmt_INSERT_QUEUElog.setLong( 1, Queue_Id );
            stmt_INSERT_QUEUElog.setString( 2, sRequest );
            stmt_INSERT_QUEUElog.registerOutParameter( 3, Types.ROWID );
            // stmt_INSERT_QUEUElog.re  // .registerReturnParameter(3, OracleTypes.ROWID);
            count = stmt_INSERT_QUEUElog.executeUpdate();
            if (count>0)
            {
              //  ROWID_QUEUElog = stmt_INSERT_QUEUElog.getRowId(4);
                ROWID_QUEUElog  = stmt_INSERT_QUEUElog.getRowId(3); //rest is not null and not empty

            }

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {

            dataAccess_log.error( "insert into ARTX_PROJ.MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + INSERT_QUEUElog_Request + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ROWID_QUEUElog;
        }
        return ROWID_QUEUElog;
    }


    public PreparedStatement  make_insert_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Details );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_INSERT_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_Temp2ErrIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(
                    "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                            "set q.Queue_Direction = '" + XMLchars.DirectERRIN +"'" +
                            ", q.Queue_Date= sysdate" +
                            ", q.Msg_Status = 0" +
                            ", q.Msg_Date= sysdate" +
                            ", q.Operation_Id=?" +
                            ", q.Outqueue_Id=? " +
                            ", q.msg_type = ?" +
                            ", q.Msg_Reason = ?" +
                            ", q.MsgDirection_Id= ?" +
                            ", q.msg_infostreamid = 0" +
                            ", q.msg_type_own = ?" +
                            ", q.msg_result = null" +
                            ", q.subsys_cod = ?" +
                            ", q.Retry_Count=0 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", q.Prev_Queue_Direction='+" + XMLchars.DirectTEMP + "'" +
                            ", q.Prev_Msg_Date=q.Msg_Date " +
                            "where 1=1 and q.Queue_Id = ?  " );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmt_UPDATE_MessageQueue_Temp2ErrIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_Temp2ErrIN(@NotNull Long Queue_Id, @NotNull Integer Operation_Id,
                                                 Integer MsgDirection_Id , String SubSys_Cod,
                                                 @NotNull String Msg_Type, String Msg_Type_own,
                                                 String Msg_Reason, Long OutQueue_Id,
                                                 Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_In: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
        //        "set q.Queue_Direction = 'IN', q.Msg_Reason = '"+ Msg_Reason+ "' " +
        //        ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count= 1 " +
        //        ", q.Prev_Queue_Direction='TMP', q.Prev_Msg_Date=q.Msg_Date " +
        // "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 1, Operation_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 2, OutQueue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 3, Msg_Type );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 4, Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason );
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 5, MsgDirection_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 6, Msg_Type_own );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 7, SubSys_Cod );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 8, Queue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]:  doUPDATE_MessageQueue_In2Ok ) fault: " + e.getMessage() );
            System.err.println( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: doUPDATE_MessageQueue_In2Ok )) fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_In2Ok( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "update ARTX_PROJ.MESSAGE_QUEUE Q " +
                            "set q.Queue_Direction = 'IN'" +
                            ", q.Queue_Date= sysdate" +
                            ", q.Msg_Status = 0" +
                            ", q.Msg_Date= sysdate" +
                            ", q.Operation_Id=?" +
                            ", q.Outqueue_Id=? " +
                            ", q.msg_type = ?" +
                            ", q.Msg_Reason = ?" +
                            ", q.MsgDirection_Id= ?" +
                            ", q.msg_infostreamid = 0" +
                            ", q.msg_type_own = ?" +
                            ", q.msg_result = null" +
                            ", q.subsys_cod = ?" +
                            ", q.Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", q.Prev_Queue_Direction='TMP'" +
                            ", q.Prev_Msg_Date=q.Msg_Date " +
                            "where 1=1 and q.Queue_Id = ?  " );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_In2Ok = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_In2Ok(@NotNull Long Queue_Id, @NotNull Integer Operation_Id,
                                            Integer MsgDirection_Id , String SubSys_Cod,
                                            @NotNull String Msg_Type, String Msg_Type_own,
                                            String Msg_Reason, Long OutQueue_Id,
                                            Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_In: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
        //        "set q.Queue_Direction = 'IN', q.Msg_Reason = '"+ Msg_Reason+ "' " +
        //        ", q.Msg_Date= sysdate,  q.Msg_Status = 0, q.Retry_Count= 1 " +
        //        ", q.Prev_Queue_Direction='TMP', q.Prev_Msg_Date=q.Msg_Date " +
        // "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 1, Operation_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 2, OutQueue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 3, Msg_Type );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 4, Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason );
            stmt_UPDATE_MessageQueue_In2Ok.setInt( 5, MsgDirection_Id );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 6, Msg_Type_own );
            stmt_UPDATE_MessageQueue_In2Ok.setString( 7, SubSys_Cod );
            stmt_UPDATE_MessageQueue_In2Ok.setLong( 8, Queue_Id );
            stmt_UPDATE_MessageQueue_In2Ok.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]:  doUPDATE_MessageQueue_In2Ok ) fault: " + e.getMessage() );
            System.err.println( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: doUPDATE_MessageQueue_In2Ok )) fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
/*
    public PreparedStatement  make_UPDATE_MessageQueue_Send2finishedOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_Send2finishedOUT );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_Send2finishedOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_Send2finishedOUT(@NotNull Long Queue_Id, String Queue_Direction,
                                                       String pMsg_Reason,
                                                       int Msg_Status, int Retry_Count,
                                                       Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_Send2finishedOUT: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
                "set q.Queue_Direction = '"+Queue_Direction+ "', q.Msg_Reason = '"+ pMsg_Reason+ "' " +
                ", q.Msg_Date= sysdate,  q.Msg_Status = "+ Msg_Status + ", q.Retry_Count= ? " +
                ", q.Prev_Queue_Direction='SEND', q.Prev_Msg_Date=q.Msg_Date " +
                "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setString( 1, Queue_Direction );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setString( 2, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setInt( 3, Msg_Status );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setInt( 4, Retry_Count );

            stmt_UPDATE_MessageQueue_Send2finishedOUT.setLong( 5, Queue_Id );

            stmt_UPDATE_MessageQueue_Send2finishedOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_Send2finishedOUT + ") fault: " + e.getMessage() );
            System.err.println( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_Send2finishedOUT + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
*/
    public PreparedStatement  make_UPDATE_MessageQueue_DirectionAsIS( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_DirectionAsIS );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_DirectionAsIS = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_DirectionAsIS(@NotNull Long Queue_Id, int Retry_interval,
                                                       String pMsg_Reason,
                                                       int Msg_Status, int Retry_Count,
                                                       Logger dataAccess_log ) {
        dataAccess_log.info("[" + Queue_Id + "] try UPDATE_MessageQueue_DirectionAsIS : ["+ UPDATE_MessageQueue_DirectionAsIS + "]" );
        dataAccess_log.info("[" + Queue_Id + "] update ARTX_PROJ.MESSAGE_QUEUE Q " +
                        "set q.Msg_Date= sysdate + ("+Retry_interval+"/(24*3600)), q.Msg_Reason = ?, q.Msg_Status = "+ Msg_Status +" , q.Retry_Count= "+ Retry_Count +", " +
                        "q.Prev_Msg_Date=q.Msg_Date where 1=1 and q.Queue_Id = " + Queue_Id.toString());
        try {
            BigDecimal queueId = new BigDecimal( Queue_Id.toString() );
            dataAccess_log.info("[" + Queue_Id + "] BigDecimal queueId =" + queueId.toString() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 1, Retry_interval );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setString( 2, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 3, Msg_Status );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 4, Retry_Count );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setBigDecimal(5, queueId );
                    // NUMBER.formattedTextToNumber( Queue_Id.toString())  ) ; //.setNUMBER( 5, NUMBER.formattedTextToNumber() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {

            dataAccess_log.error( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: " + e.getMessage() );
            System.err.println( "update ARTX_PROJ.MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_DELETE_Message_Confirmation( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        PreparedStatement StmtMsg_QueueH;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Confirmation );
            StmtMsg_QueueH = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_ConfirmationH );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Confirmation = StmtMsg_Queue;
        this.stmt_DELETE_Message_ConfirmationH = StmtMsg_QueueH;
        return  StmtMsg_Queue ;
    }

    public  int doDELETE_Message_Confirmation(@NotNull long Queue_Id, Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doDELETE_Message_Confirmation!" );
        try {
                // сначала удаляем всЁ, что растет из Confirmation
            stmt_DELETE_Message_Confirmation.setLong( 1, Queue_Id );
            stmt_DELETE_Message_Confirmation.setLong( 2, Queue_Id );
            stmt_DELETE_Message_Confirmation.executeUpdate();
                 // а теперь и сам Confirmation tag

            stmt_DELETE_Message_ConfirmationH.setLong( 1, Queue_Id );
            stmt_DELETE_Message_ConfirmationH.executeUpdate();

            Hermes_Connection.commit();

        } catch (Exception e) {
            dataAccess_log.error( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: " + e.getMessage() );
            System.err.println( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_delete_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Details );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }


/*
    public PreparedStatement  make_Message_Update_Queue_Queue_Date4Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Queue_Date4Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Queue_Date4Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_Message_Update_Out2Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Out2Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Out2Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int  doUPDATE_MessageQueue_Queue_Date4Send( long Queue_Id,   Logger dataAccess_log ) {
        dataAccess_log.info("[" + Queue_Id + "] doUPDATE_MessageQueue_Queue_Date4Send()" );
        try {

            stmtUPDATE_MessageQueue_Queue_Date4Send.setLong( 1, Queue_Id );
            stmtUPDATE_MessageQueue_Queue_Date4Send.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;

    }

    public  int doUPDATE_MessageQueue_Out2Send( long Queue_Id,  String pMsg_Reason, Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_Out2Send:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Out2Send.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Out2Send.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_Out2Send.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " );
                    e.printStackTrace();
            return -1;
        }
        return 0;
    }
    */

    public PreparedStatement  make_Message_Update_Send2ErrorOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2ErrorOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2ErrorOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Send2ErrorOUT(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setString( 1,  pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason  );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    /*
    public PreparedStatement  make_UPDATE_MessageQueue_Send2AttOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2AttOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2AttOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Send2AttOUT(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Send2AttOUT.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2AttOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2AttOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
    */

    /*
    public PreparedStatement  make_UPDATE_MessageQueue_SetMsg_Reason( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_SetMsg_Reason );
        } catch (SQLException e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_SetMsg_Reason = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_SetMsg_Reason(Long Queue_Id, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_SetMsg_Reason.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_SetMsg_Reason.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
*/

    public PreparedStatement  make_Message_Update_ExeIn2DelIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_ExeIn2DelIN );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_ExeIn2DelIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_ExeIn2DelIN(Long Queue_Id, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Out2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_ExeIn2DelIN.setLong( 1, Queue_Id );
            stmtUPDATE_MessageQueue_ExeIn2DelIN.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_ExeIn2DelIN + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public PreparedStatement  make_Message_Update_In2ErrorIN( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_In2ErrorIN );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_In2ErrorIN = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_In2ErrorIN(Long Queue_Id, String pMsg_Reason, Integer pMsg_Status, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Out2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_In2ErrorIN.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_In2ErrorIN.setInt( 2, pMsg_Status );
            stmtUPDATE_MessageQueue_In2ErrorIN.setLong( 3, Queue_Id );
            stmtUPDATE_MessageQueue_In2ErrorIN.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_In2ErrorIN + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_Message_Update_Out2ErrorOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {

            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Out2ErrorOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtUPDATE_MessageQueue_Out2ErrorOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Out2ErrorOUT(Long Queue_Id, String pMsg_Reason, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Out2ErrorOUT:" + pMsg_Reason );
        try {
            stmtUPDATE_MessageQueue_Out2ErrorOUT.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Out2ErrorOUT.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_Out2ErrorOUT.executeUpdate();

            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public PreparedStatement  make_Message_QueryConfirmation( Logger dataAccess_log ) {
        PreparedStatement stmtMsgQueueConfirmationDet;
        try {

            stmtMsgQueueConfirmationDet = this.Hermes_Connection.prepareStatement(
        "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from artx_proj.message_queuedet D where (1=1)  and d.QUEUE_ID = ? and d.Tag_Num >= ? order by   Tag_Par_Num, Tag_Num "
        );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmationDet = stmtMsgQueueConfirmationDet;
        return  stmtMsgQueueConfirmationDet ;
    }

    public PreparedStatement  make_Message_LastBodyTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select Tag_Num from (" +
                            " select Tag_Num from (" +
                                    " select Tag_Num from  artx_proj.message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation'" +
                                    " union all" +
                                    " select max(Tag_Num) + 1  as  Tag_Num from  artx_proj.message_queuedet  WHERE QUEUE_ID = ?" +
                            " ) order by Tag_Num" +
                    " ) where rownum =1"
                    );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgLastBodyTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_Message_ConfirmationTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                            "select Tag_Num from ( select Tag_Num from  artx_proj.message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation' order by Tag_Num desc) " +
                                    "where rownum=1"
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmationTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }


    public PreparedStatement  make_MessageBody_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from artx_proj.message_queuedet D" +
                            " where (1=1) and d.QUEUE_ID = ? and d.Tag_Id < ?  order by   Tag_Par_Num, Tag_Num "
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueBody = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_MessageConfirmation_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

            StmtMsgQueueDet = this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num" +
                            " from artx_proj.message_queuedet D" +
                            " where (1=1)" +
                            " and d.QUEUE_ID = ? and d.Tag_Id >= ? " +
                            " order by   Tag_Par_Num, Tag_Num "
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmation = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement make_Message_Query(  Logger dataAccess_log ) {
    PreparedStatement stmtMsgQueue;

    String selectMessageStatement =
            "select Q.queue_direction," +
                    " Q.msg_status," +
                    " Q.outqueue_id," +
                    " Q.msg_reason," +
                    " Q.msg_result " +
                    "from ARTX_PROJ.MESSAGE_QUEUE q" +
                    " Where  Q.queue_id = ?";
    try {
        dataAccess_log.info("MESSAGE_QueueSelect:\n" + selectMessageStatement);
        stmtMsgQueue = this.Hermes_Connection.prepareStatement(selectMessageStatement);


    } catch (Exception e) {
        dataAccess_log.error(e.getMessage());
        e.printStackTrace();
        return ((PreparedStatement) null);
    }
    this.stmtMsgQueue = stmtMsgQueue;
    return stmtMsgQueue;
    }

}
