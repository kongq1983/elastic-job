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

import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationNode;
import org.apache.shardingsphere.elasticjob.infra.pojo.JobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerNode;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

/**
 * Sharding listener manager.
 */
public final class ShardingListenerManager extends AbstractListenerManager {
    
    private final String jobName;
    
    private final ConfigurationNode configNode;
    
    private final InstanceNode instanceNode;
    
    private final ServerNode serverNode;
    
    private final ShardingService shardingService;
    
    public ShardingListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
        serverNode = new ServerNode(jobName);
        shardingService = new ShardingService(regCenter, jobName);
    }
    
    @Override
    public void start() { // 监听的是/jobName
        addDataListener(new ShardingTotalCountChangedJobListener());
        addDataListener(new ListenServersChangedJobListener());
    }
    // /jobname/config配置文件变动 触发
    class ShardingTotalCountChangedJobListener extends AbstractJobListener {
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (configNode.isConfigPath(path) && 0 != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) {
                int newShardingTotalCount = YamlEngine.unmarshal(data, JobConfigurationPOJO.class).toJobConfiguration().getShardingTotalCount(); // 新的分片数（配置文件变了）
                if (newShardingTotalCount != JobRegistry.getInstance().getCurrentShardingTotalCount(jobName)) { // 数量不一致
                    shardingService.setReshardingFlag(); // 设置需要分片标志
                    JobRegistry.getInstance().setCurrentShardingTotalCount(jobName, newShardingTotalCount); // 设置新的分片总数
                }
            }
        }
    }
    // 任务服务器数量事件
    class ListenServersChangedJobListener extends AbstractJobListener {
        
        @Override // jobname/instances下面有新增、删除节点触发 // NODE_CREATED  NODE_DELETED 要么有新的servers节点  或者 是否是服务器路径jobname/servers/${ip}
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (!JobRegistry.getInstance().isShutdown(jobName) && (isInstanceChange(eventType, path) || isServerChange(path))) {
                shardingService.setReshardingFlag();
            }
        }
        // isInstancePath: jobname/instances
        private boolean isInstanceChange(final Type eventType, final String path) {
            return instanceNode.isInstancePath(path) && Type.NODE_CHANGED != eventType; // NODE_CREATED  NODE_DELETED
        }
        
        private boolean isServerChange(final String path) {
            return serverNode.isServerPath(path);
        }
    }
}
