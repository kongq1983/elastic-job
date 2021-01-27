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

package org.apache.shardingsphere.elasticjob.lite.internal.election;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.LeaderExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;

/** 主节点服务 /${jobname}/leader/election   下面有latch、ecection、failover、sharding节点
 * Leader service.
 */
@Slf4j
public final class LeaderService {
    
    private final String jobName;
    
    private final ServerService serverService;
    
    private final JobNodeStorage jobNodeStorage;
    
    public LeaderService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
    }
    
    /** 选取主节点
     * Elect leader.
     */
    public void electLeader() {
        log.debug("Elect a new leader now.");
        jobNodeStorage.executeInLeader(LeaderNode.LATCH, new LeaderElectionExecutionCallback());
        log.debug("Leader election completed.");
    }
    
    /** 当前节点是否主节点
     * Judge current server is leader or not.
     * 
     * <p>
     * If leader is electing, this method will block until leader elected success.
     * </p>
     * 
     * @return current server is leader or not
     */
    public boolean isLeaderUntilBlock() {
        while (!hasLeader() && serverService.hasAvailableServers()) { ///${JOB_NAME}/leader/electron/instance不存在 && 有可用的服务器
            log.info("Leader is electing, waiting for {} ms", 100);
            BlockUtils.waitingShortTime(); //休息100ms
            if (!JobRegistry.getInstance().isShutdown(jobName) && serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getIp())) {
                electLeader();
            }
        }
        return isLeader(); //到这里 肯定有leader了
    }
    
    /** 当前server是否是leader
     * Judge current server is leader or not.
     *
     * @return current server is leader or not
     */
    public boolean isLeader() {
        return !JobRegistry.getInstance().isShutdown(jobName) && JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId().equals(jobNodeStorage.getJobNodeData(LeaderNode.INSTANCE));
    }
    
    /** 判断是否已经有主节点  /${JOB_NAME}/leader/electron/instance是否存在
     * Judge has leader or not in current time.
     * 
     * @return has leader or not in current time
     */
    public boolean hasLeader() {
        return jobNodeStorage.isJobNodeExisted(LeaderNode.INSTANCE); // /${JOB_NAME}/leader/electron/instance
    }
    
    /**
     * Remove leader and trigger leader election.
     */
    public void removeLeader() {
        jobNodeStorage.removeJobNodeIfExisted(LeaderNode.INSTANCE);
    }
    
    @RequiredArgsConstructor
    class LeaderElectionExecutionCallback implements LeaderExecutionCallback {
        
        @Override
        public void execute() {
            if (!hasLeader()) { // 当前无主节点  创建临时节点/${JOB_NAME}/leader/electron/instance 为当前节点 值为${jobInstanceId}
                jobNodeStorage.fillEphemeralJobNode(LeaderNode.INSTANCE, JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId()); // 成为leader
            }
        }
    }
}
