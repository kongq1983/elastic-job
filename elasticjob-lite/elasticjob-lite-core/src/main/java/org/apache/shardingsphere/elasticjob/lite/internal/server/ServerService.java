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

package org.apache.shardingsphere.elasticjob.lite.internal.server;

import com.google.common.base.Strings;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;

import java.util.List;

/** 作业服务器服务  /${jobName}/servers
 * Server service.
 */
public final class ServerService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final ServerNode serverNode;
    
    public ServerService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        serverNode = new ServerNode(jobName);
    }
    
    /** 如果当前jobName已经shutdown了  则 ${jobName}/servers/${ip} 的 值设置ENABLED或DISABLED
     * Persist online status of job server.
     * 
     * @param enabled enable server or not
     */
    public void persistOnline(final boolean enabled) {
        if (!JobRegistry.getInstance().isShutdown(jobName)) { //如果当前jobName已经shutdown了  则 ${jobName}/servers/${ip} 的 值设置ENABLED或DISABLED
            jobNodeStorage.fillJobNode(serverNode.getServerNode(JobRegistry.getInstance().getJobInstance(jobName).getIp()), enabled ? ServerStatus.ENABLED.name() : ServerStatus.DISABLED.name());
        }
    }
    
    /** 是否有可用的服务器(ENABLED)
     * Judge has available servers or not.
     * 
     * @return has available servers or not
     */
    public boolean hasAvailableServers() {
        List<String> servers = jobNodeStorage.getJobNodeChildrenKeys(ServerNode.ROOT);  //  /${jobName}/servers
        for (String each : servers) {
            if (isAvailableServer(each)) {
                return true;
            }
        }
        return false;
    }
    
    /** 当前ip是否是可用的
     * Judge is available server or not.
     * 
     * @param ip job server IP address
     * @return is available server or not
     */
    public boolean isAvailableServer(final String ip) {
        return isEnableServer(ip) && hasOnlineInstances(ip); // 条件1： /${jobName}/servers/${ip} 的 值:ENABLED  当前ip实例，只要有1个是正常的
    }
    
    private boolean hasOnlineInstances(final String ip) { //该ip是否有在线的服务实例
        for (String each : jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT)) {  //  /${jobName}/instances/${instanceId}
            if (each.startsWith(ip)) {  // each 比如 192.16.67.21-@1234
                return true;
            }
        }
        return false;
    }
    
    /**  /${jobName}/servers/${ip} 这个路径一定要存在，否则一直等候，一直取
     * Judge is server enabled or not.
     *
     * @param ip job server IP address
     * @return is server enabled or not    true:enabled
     */
    public boolean isEnableServer(final String ip) {
        String serverStatus = jobNodeStorage.getJobNodeData(serverNode.getServerNode(ip)); //  /${jobName}/servers/${ip}  值:ENABLED
        while (Strings.isNullOrEmpty(serverStatus)) { //返回是空值
            BlockUtils.waitingShortTime(); //等候100ms
            serverStatus = jobNodeStorage.getJobNodeData(serverNode.getServerNode(ip)); // 重新读取/${jobName}/servers/${ip}的值
        }
        return !ServerStatus.DISABLED.name().equals(serverStatus);
    }
}
