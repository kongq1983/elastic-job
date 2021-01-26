package com.kq.elasticjob.simple;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.ScheduleJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperConfiguration;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperRegistryCenter;

import java.util.concurrent.TimeUnit;

/**
 * @author kq
 * @date 2020-08-27 10:02
 * @since 2020-0630
 */
public class SimpleJobDemo {

    public static void main(String[] args) throws Exception{
        // 调度基于 class 类型的作业
        new ScheduleJobBootstrap(createRegistryCenter(), new MyElasticJob(), createJobConfiguration()).schedule();
        // 调度基于 type 类型的作业
//        new ScheduleJobBootstrap(createRegistryCenter(), "MY_TYPE", createJobConfiguration()).schedule();

        TimeUnit.MINUTES.sleep(60);

    }

    private static CoordinatorRegistryCenter createRegistryCenter() {
        CoordinatorRegistryCenter regCenter = new ZookeeperRegistryCenter(new ZookeeperConfiguration(ServerConfig.ZK_SERVER, "elastic-job-demo"));
        regCenter.init();
        return regCenter;
    }

    private static JobConfiguration createJobConfiguration() {
        // 创建作业配置
        // "0/5 * * * * ?",
        JobConfiguration simpleCoreConfig = JobConfiguration.newBuilder("jobdemo", 3)
                .shardingItemParameters("0=A,1=B,2=C").cron("* 0/5 * * * ?").failover(true).misfire(true).build();

        return simpleCoreConfig;

    }

}
