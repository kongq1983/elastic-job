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

package org.apache.shardingsphere.elasticjob.lite.internal.sharding;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategy;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategyFactory;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.TransactionExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** 作业分片服务 重新分配标志:/${JOB_NAME}/leader/sharding/necessary(持久节点)
 * Sharding service.
 */
@Slf4j
public final class ShardingService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final LeaderService leaderService;
    
    private final ConfigurationService configService;
    
    private final InstanceService instanceService;
    
    private final ServerService serverService;
    
    private final ExecutionService executionService;

    private final JobNodePath jobNodePath;
    
    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }
    
    /** 设置需要重新分片的标记
     * Set resharding flag.
     */
    public void setReshardingFlag() {
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY); // 持久节点 /${JOB_NAME}/leader/sharding/necessary
    }
    
    /** 判断是否需要重新分片
     * Judge is need resharding or not.
     * 
     * @return is need resharding or not
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY); // /${JOB_NAME}/leader/sharding/necessary
    }
    
    /**
     * Sharding if necessary.
     * 是否要分片-分片核心逻辑处理
     * <p> leader处理分片，非leader休息
     * Sharding if current job server is leader server;
     * Do not sharding if no available job server. 
     * </p>
     */
    public void shardingIfNecessary() {
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances(); // 获取可用的instances，会先判断server是否是enabled
        if (!isNeedSharding() || availableJobInstances.isEmpty()) { // // 判断是否需要重新分片(necessary节点存在)  或者  没有可分配的实例
            return;
        }
        if (!leaderService.isLeaderUntilBlock()) { // 非leader会进入if逻辑
            blockUntilShardingCompleted(); // 当前节点不是主节点 并且正在分片中  则等待分片结束 ${namespace}/jobname/leader/sharding/necessary节点存在 或 ${namespace}/jobname/leader/sharding/processing 节点存在(表示正在执行分片操作)
            return;
        } // 下面只有leader才会执行
        waitingOtherShardingItemCompleted(); //有正在运行的实例  会sleep(100L)
        JobConfiguration jobConfig = configService.load(false); // 到这里说明没有正在运行的实例
        int shardingTotalCount = jobConfig.getShardingTotalCount(); // 总的分片数
        log.debug("Job '{}' sharding begin.", jobName);
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, ""); // 创建${namespace}/jobname/leader/sharding/processing 主节点在分片时持有的节点(临时节点)
        resetShardingInfo(shardingTotalCount); // 删除原有的分片instance，重新创建新的分片jobname/sharding/${item}
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(jobConfig.getJobShardingStrategyType()); // 获取分片策略
        jobNodeStorage.executeInTransaction(new PersistShardingInfoTransactionExecutionCallback(jobShardingStrategy.sharding(availableJobInstances, jobName, shardingTotalCount)));
        log.debug("Job '{}' sharding complete.", jobName); //上面这句 会创建jobname/sharding/${item}/instance 节点    分片结束删除necessary、processing节点
    }
    
    private void blockUntilShardingCompleted() { // 非leader 并且 ${namespace}/jobname/leader/sharding/necessary节点存在 或 ${namespace}/jobname/leader/sharding/processing 节点存在(表示正在执行分片操作)
        while (!leaderService.isLeaderUntilBlock() && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY) || jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    
    private void waitingOtherShardingItemCompleted() {
        while (executionService.hasRunningItems()) { // 有正在运行的实例,则休息一下
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    // jobName/sharding/  删除原有的分片instance，重新创建分片instance信息
    private void resetShardingInfo(final int shardingTotalCount) {
        for (int i = 0; i < shardingTotalCount; i++) {
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i)); // 删除 jobname/sharding/${item}/instance
            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i); // 不存在则创建jobname/sharding/${item}
        }
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size(); // jobname/sharding树下,实际有几个子节点
        if (actualShardingTotalCount > shardingTotalCount) { //实际大于配置的分片数
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i); // 删除多余的分片数
            }
        }
    }
    
    /**
     * Get sharding items.
     *
     * @param jobInstanceId job instance ID
     * @return sharding items
     */
    public List<Integer> getShardingItems(final String jobInstanceId) {
        JobInstance jobInstance = new JobInstance(jobInstanceId);
        if (!serverService.isAvailableServer(jobInstance.getIp())) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (jobInstance.getJobInstanceId().equals(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                result.add(i);
            }
        }
        return result;
    }
    
    /**
     * Get sharding items from localhost job server.
     * 
     * @return sharding items from localhost job server
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
            return Collections.emptyList();
        }
        return getShardingItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * Query has sharding info in offline servers or not.
     * 
     * @return has sharding info in offline servers or not
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT);
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }
    
    @RequiredArgsConstructor
    class PersistShardingInfoTransactionExecutionCallback implements TransactionExecutionCallback {
        
        private final Map<JobInstance, List<Integer>> shardingResults;
        
        @Override
        public List<CuratorOp> createCuratorOperators(final TransactionOp transactionOp) throws Exception {
            List<CuratorOp> result = new LinkedList<>();
            for (Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
                for (int shardingItem : entry.getValue()) { //创建instaance节点  key: jobName/sharding/0/instance  value: 172.16.5.1@-@2908
                    result.add(transactionOp.create().forPath(jobNodePath.getFullPath(ShardingNode.getInstanceNode(shardingItem)), entry.getKey().getJobInstanceId().getBytes()));
                }
            }
            result.add(transactionOp.delete().forPath(jobNodePath.getFullPath(ShardingNode.NECESSARY))); // 删除leader/sharding/necessary
            result.add(transactionOp.delete().forPath(jobNodePath.getFullPath(ShardingNode.PROCESSING))); // 删除leader/sharding/processing
            return result; // 分片结束
        }
    }
}
