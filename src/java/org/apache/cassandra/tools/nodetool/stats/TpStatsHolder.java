/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool.stats;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;

public class TpStatsHolder implements StatsHolder
{
    public final NodeProbe probe;

    public TpStatsHolder(NodeProbe probe)
    {
        this.probe = probe;
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        HashMap<String, Object> result = new HashMap<>();
        TreeMap<String, Map<String, Object>> threadPools = new TreeMap<>();
        HashMap<String, Object> droppedMessage = new HashMap<>();
        HashMap<String, double[]> waitLatencies = new HashMap<>();

        for (Map.Entry<String, String> tp : probe.getThreadPools().entries())
        {
            Map<String, Object> threadPool = new HashMap<>();
            threadPool.put("ActiveTasks", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "ActiveTasks"));
            threadPool.put("PendingTasks", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "PendingTasks"));
            threadPool.put("CompletedTasks", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "CompletedTasks"));
            threadPool.put("CurrentlyBlockedTasks", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "CurrentlyBlockedTasks"));
            threadPool.put("TotalBlockedTasks", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "TotalBlockedTasks"));
            threadPool.put("CPU", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "CPU")); // ms/sec
            threadPool.put("Allocations", probe.getThreadPoolMetric(tp.getKey(), tp.getValue(), "Allocations")); // mb/sec
            threadPools.put(tp.getValue(), threadPool);
        }
        result.put("ThreadPools", threadPools);

        for (Map.Entry<String, Integer> entry : probe.getDroppedMessages().entrySet())
        {
            droppedMessage.put(entry.getKey(), entry.getValue());
            try
            {
                waitLatencies.put(entry.getKey(), probe.metricPercentilesAsArray(probe.getMessagingQueueWaitMetrics(entry.getKey())));
            }
            catch (RuntimeException e)
            {
                // ignore the exceptions when fetching metrics
            }
        }

        result.put("DroppedMessage", droppedMessage);
        result.put("WaitLatencies", waitLatencies);

        return result;
    }
}
