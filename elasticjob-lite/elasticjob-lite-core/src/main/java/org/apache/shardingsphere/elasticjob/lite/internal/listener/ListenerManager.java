/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.lite.internal.listener;

import org.apache.shardingsphere.elasticjob.api.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.config.RescheduleListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.election.ElectionListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.failover.FailoverListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.guarantee.GuaranteeListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.ShutdownListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.TriggerListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.MonitorExecutionListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ShardingListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.List;

/**
 * Listener manager facade.
 */
public final class ListenerManager {
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ElectionListenerManager electionListenerManager;
    
    private final ShardingListenerManager shardingListenerManager;
    
    private final FailoverListenerManager failoverListenerManager;
    
    private final MonitorExecutionListenerManager monitorExecutionListenerManager;
    
    private final ShutdownListenerManager shutdownListenerManager;
    
    private final TriggerListenerManager triggerListenerManager;
    
    private final RescheduleListenerManager rescheduleListenerManager;

    private final GuaranteeListenerManager guaranteeListenerManager;
    
    private final RegistryCenterConnectionStateListener regCenterConnectionStateListener;
    
    public ListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners) {
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        electionListenerManager = new ElectionListenerManager(regCenter, jobName);
        shardingListenerManager = new ShardingListenerManager(regCenter, jobName);
        failoverListenerManager = new FailoverListenerManager(regCenter, jobName);
        monitorExecutionListenerManager = new MonitorExecutionListenerManager(regCenter, jobName);
        shutdownListenerManager = new ShutdownListenerManager(regCenter, jobName);
        triggerListenerManager = new TriggerListenerManager(regCenter, jobName);
        rescheduleListenerManager = new RescheduleListenerManager(regCenter, jobName);
        guaranteeListenerManager = new GuaranteeListenerManager(regCenter, jobName, elasticJobListeners);
        regCenterConnectionStateListener = new RegistryCenterConnectionStateListener(regCenter, jobName);
    }
    
    /** 新节点启动的时候
     * Start all listeners.
     */
    public void startAllListeners() {
        electionListenerManager.start(); // //主节点选举
        shardingListenerManager.start(); //分片
        failoverListenerManager.start(); //失效转移
        monitorExecutionListenerManager.start();
        shutdownListenerManager.start(); //关闭
        triggerListenerManager.start(); //触发器
        rescheduleListenerManager.start(); //重新调度
        guaranteeListenerManager.start(); // //保证分布式任务全部开始和结束状态
        jobNodeStorage.addConnectionStateListener(regCenterConnectionStateListener); //注册中心与任务节点的连接状态
    }
}
