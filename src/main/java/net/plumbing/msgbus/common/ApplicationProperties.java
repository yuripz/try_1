package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;

public  class ApplicationProperties {
    public static String HrmsPoint;
    public static String hrmsDbLogin;
    public static String hrmsDbPasswd;
    public static Long TotalTimeTasks;
    public static Integer WaitTimeBetweenScan;
    public static Integer ApiRestWaitTime;
    public static Integer ShortRetryCount;
    public static Integer LongRetryCount;
    public static Integer ShortRetryInterval;
    public static Integer LongRetryInterval ;




    public void setWaitTimeBetweenScan(Integer waitTimeBetweenScan) {
        this.WaitTimeBetweenScan = waitTimeBetweenScan;
    }

    public void setHrmsPoint(String hrmspoint) {
        this.HrmsPoint = hrmspoint;
    }

    public void setTotalTimeTasks(Long totalTimeTasks) {
        this.TotalTimeTasks = totalTimeTasks;
    }

    public void setHrmsDbPasswd(String hrmsDbPasswd) {
        this.hrmsDbPasswd = hrmsDbPasswd;
    }

    public void setHrmsDbLogin(String hrmsDbLogin) {
        this.hrmsDbLogin = hrmsDbLogin;
    }

    public void setApiRestWaitTime( int ApiRestWaitTime) { this.ApiRestWaitTime = ApiRestWaitTime; }

    @Autowired
    public static HikariDataSource dataSource; //= HiDataSource();
}
