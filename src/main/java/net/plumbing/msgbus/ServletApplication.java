package net.plumbing.msgbus;


import net.plumbing.msgbus.config.ConnectionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import org.springframework.boot.CommandLineRunner;

import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.HikariDataAccess;
import net.plumbing.msgbus.config.DBLoggingProperties;


import net.plumbing.msgbus.init.InitMessageRepository;
import net.plumbing.msgbus.common.DataAccess;
import net.plumbing.msgbus.monitoring.AppendDataAccess;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageType;

// import static org.springframework.boot.jdbc.DataSourceBuilder.*;

/*
@SpringBootApplication
public class ServletApplication {
    @Autowired
    public DataSource dataSource;
    public static final Logger AppThead_log = LoggerFactory.getLogger(ServletApplication.class);

    public static void main(String[] args) {

        SpringApplication.run(ServletApplication.class, args);
    }
}
*/
@SpringBootApplication

public class ServletApplication implements CommandLineRunner {
    @Autowired

    public static final Logger AppThead_log = LoggerFactory.getLogger(ServletApplication.class);

    @Autowired
    public ConnectionProperties connectionProperties;
    @Autowired
    public DBLoggingProperties dbLoggingProperties;


    public static void main(String[] args) throws Exception {
        SpringApplication.run(ServletApplication.class, args);
    }
    @Override
    public void run(String... args) throws Exception {

        AppThead_log.warn(dbLoggingProperties.toString());
        AppThead_log.warn(connectionProperties.toString());
        String propLongRetryCount = connectionProperties.getlongRetryCount();
        if (propLongRetryCount == null) propLongRetryCount = "12";
        String propShortRetryCount = connectionProperties.getshortRetryCount();
        if (propShortRetryCount == null) propShortRetryCount = "3";

        String propLongRetryInterval = connectionProperties.getlongRetryInterval();
        if (propLongRetryInterval == null) propLongRetryInterval = "600";
        String propShortRetryInterval = connectionProperties.getshortRetryInterval();
        if (propShortRetryInterval == null) propShortRetryInterval = "30";

        int ShortRetryCount = Integer.parseInt(connectionProperties.getshortRetryCount() );
        ApplicationProperties.ShortRetryCount = ShortRetryCount;
        int LongRetryCount = Integer.parseInt( connectionProperties.getlongRetryCount()  );
        ApplicationProperties.LongRetryCount = LongRetryCount;
        int ShortRetryInterval = Integer.parseInt( connectionProperties.getshortRetryInterval() );
        ApplicationProperties.ShortRetryInterval = ShortRetryInterval;
        int LongRetryInterval = Integer.parseInt( connectionProperties.getlongRetryInterval() );
        ApplicationProperties.LongRetryInterval = LongRetryInterval;
        int WaitTimeBetweenScan = Integer.parseInt( connectionProperties.getwaitTimeScan() );
        ApplicationProperties.WaitTimeBetweenScan = WaitTimeBetweenScan;
        int NumMessageInScan = Integer.parseInt( connectionProperties.getnumMessageInScan() );
        int ApiRestWaitTime = Integer.parseInt( connectionProperties.getapiRestWaitTime() );
        ApplicationProperties.ApiRestWaitTime = ApiRestWaitTime;
        ApplicationProperties.HrmsPoint =  connectionProperties.gethrmsPoint() ;
        ApplicationProperties.hrmsDbLogin = connectionProperties.gethrmsDbLogin();
        ApplicationProperties.hrmsDbPasswd =  connectionProperties.gethrmsDbPasswd();

        int FirstInfoStreamId = 101;
        if ( connectionProperties.getfirstInfoStreamId() != null)
            FirstInfoStreamId = Integer.parseInt( connectionProperties.getfirstInfoStreamId() );
        String psqlFunctionRun = connectionProperties.getpsqlFunctionRun();

        ApplicationProperties.dataSource = HikariDataAccess.HiDataSource (connectionProperties.gethrmsPoint(),
                connectionProperties.gethrmsDbLogin(),
                connectionProperties.gethrmsDbPasswd()
        );

        AppThead_log.info("DataSource = " + ApplicationProperties.dataSource );
        if ( ApplicationProperties.dataSource != null )
        {
            AppThead_log.info("DataSource = " + ApplicationProperties.dataSource
                    + " JdbcUrl:" + ApplicationProperties.dataSource.getJdbcUrl()
                    + " isRunning:" + ApplicationProperties.dataSource.isRunning() );
        }


        // Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z
        DataAccess.make_Hermes_Connection( connectionProperties.gethrmsPoint(),
                connectionProperties.gethrmsDbLogin(),
                connectionProperties.gethrmsDbPasswd(),
                AppThead_log
        );
        // Зачитываем MessageDirection


        InitMessageRepository.SelectMsgDirections(ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval,
                AppThead_log );

        //AppThead_log.info("keysAllMessageDirections: " + MessageDirections.AllMessageDirections.get(1).getMsgDirection_Desc() );
        int TotalNumTasks= Integer.parseInt( connectionProperties.gettotalNumTasks() );
        Long TotalTimeTasks = Long.parseLong( connectionProperties.gettotalTimeTasks());
        Long intervalReInit = Long.parseLong( connectionProperties.getintervalReInit());


        InitMessageRepository.SelectMsgTypes( AppThead_log );
        AppThead_log.info("keysAllMessageDirections: " + MessageType.AllMessageType.get(1).getMsg_TypeDesc() );

        InitMessageRepository.SelectMsgTemplates( AppThead_log );
        AppThead_log.info("keysAllMessageTemplates: " + MessageTemplate.AllMessageTemplate.get(1).getTemplate_name() );

        // int totalTasks = Integer.parseInt( "1" ); // TotalNumTasks; //Integer.parseInt( "50" ); //
        Long CurrentTime;
        CurrentTime = DataAccess.getCurrentTime( AppThead_log );
        DataAccess.InitDate.setTime( CurrentTime );
        AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );

        Integer count = 1;

        String CurrentTimeString;
        Long timeToReInit;

        // Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z
        AppendDataAccess.make_Monitoring_Connection( dbLoggingProperties.getdataSourceClassName(),
                dbLoggingProperties.getjdbcUrl(),
                dbLoggingProperties.getmntrDbLogin(),
                dbLoggingProperties.getmntrDbPasswd(),
                AppThead_log
        );

        for (;;) {

            AppThead_log.info("Active Threads : " + count);
            try {

                // Thread.sleep(25000);
                Thread.sleep(100000);
                CurrentTime = DataAccess.getCurrentTime( AppThead_log );
                CurrentTimeString = DataAccess.getCurrentTimeString( AppThead_log );

                timeToReInit = (CurrentTime - DataAccess.InitDate.getTime())/1000;
                if ( timeToReInit > intervalReInit )
                {
                    AppThead_log.info("CurrentTimeString=" +  CurrentTimeString + " (CurrentTime - DataAccess.InitDate.getTime())/1000: " +timeToReInit.toString() );
                    InitMessageRepository.ReReadMsgDirections( CurrentTime, ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval, AppThead_log );
                    InitMessageRepository.ReReadMsgTypes(  AppThead_log );
                    InitMessageRepository.ReReadMsgTemplates(AppThead_log);
                    DataAccess.InitDate.setTime( CurrentTime );
                    AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );

                    // если в
                    if (( psqlFunctionRun != null) && ( !psqlFunctionRun.equalsIgnoreCase("NONE")) )
                        DataAccess.moveERROUT2RESOUT( psqlFunctionRun, AppThead_log );
                }
            } catch (InterruptedException e) {
                AppThead_log.error("надо taskExecutor.shutdown! " + e.getMessage());
                e.printStackTrace();
                count = 0; // надо taskExecutor.shutdown();
                break;
            }
            if (count == 0) {
                /// taskExecutor.shutdown();
                break;
            }
        }

    }

/* */

}


