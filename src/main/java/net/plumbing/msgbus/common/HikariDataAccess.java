package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.plumbing.msgbus.ServletApplication;
import org.springframework.context.annotation.Bean;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class HikariDataAccess {
    @Bean (destroyMethod = "close")
    public static  HikariDataSource HiDataSource( String JdbcUrl, String Username, String Password ){
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        hikariConfig.setJdbcUrl( "jdbc:oracle:thin:@"+ JdbcUrl); //("jdbc:oracle:thin:@//10.242.36.8:1521/hermes12");
        hikariConfig.setUsername( Username ); //("ARTX_PROJ");
        hikariConfig.setPassword( Password ); // ("rIYmcN38St5P");

        hikariConfig.setMaximumPoolSize(50);
        hikariConfig.setConnectionTestQuery("SELECT 1 from dual");
        hikariConfig.setPoolName("springHikariCP");

        hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "2048");
        hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.autoCommit", "false");

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        try {
            Connection tryConn = dataSource.getConnection();
            PreparedStatement prepareStatement = tryConn.prepareStatement( "SELECT 1 from dual");
            prepareStatement.executeQuery();
            prepareStatement.close();
            tryConn.close();
            ServletApplication.AppThead_log.info( "getJdbcUrl: "+ hikariConfig.getJdbcUrl());
        }
        catch (java.sql.SQLException e)
        { ServletApplication.AppThead_log.error( e.getMessage());};
        ;

        return dataSource;
    }
    /* */
}
