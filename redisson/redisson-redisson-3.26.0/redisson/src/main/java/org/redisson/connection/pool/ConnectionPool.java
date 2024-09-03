/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.connection.pool;

import org.redisson.api.NodeType;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.config.MasterSlaveServersConfig;
import org.redisson.connection.ClientConnectionsEntry;
import org.redisson.connection.ClientConnectionsEntry.FreezeReason;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.ConnectionsHolder;
import org.redisson.connection.MasterSlaveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Base connection pool class 
 * 
 * @author Nikita Koksharov
 *
 * @param <T> - connection type
 */
abstract class ConnectionPool<T extends RedisConnection> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final Queue<ClientConnectionsEntry> entries = new ConcurrentLinkedQueue<>();

    final ConnectionManager connectionManager;

    final MasterSlaveServersConfig config;

    final MasterSlaveEntry masterSlaveEntry;

    ConnectionPool(MasterSlaveServersConfig config, ConnectionManager connectionManager, MasterSlaveEntry masterSlaveEntry) {
        this.config = config;
        this.masterSlaveEntry = masterSlaveEntry;
        this.connectionManager = connectionManager;
    }

    public final void addEntry(ClientConnectionsEntry entry) {
        entries.add(entry);
    }

    protected abstract ConnectionsHolder<T> getConnectionHolder(ClientConnectionsEntry entry);

    public CompletableFuture<T> get(RedisCommand<?> command) {
        List<ClientConnectionsEntry> entriesCopy = new LinkedList<ClientConnectionsEntry>(entries);
        for (Iterator<ClientConnectionsEntry> iterator = entriesCopy.iterator(); iterator.hasNext();) {
            ClientConnectionsEntry entry = iterator.next();
            if (!((!entry.isFreezed() || entry.isMasterForRead()) 
                    && isHealthy(entry))) {
                iterator.remove();
            }
        }
        if (!entriesCopy.isEmpty()) {
            ClientConnectionsEntry entry = config.getLoadBalancer().getEntry(entriesCopy, command);
            return acquireConnection(command, entry);
        }
        
        List<InetSocketAddress> failed = new LinkedList<>();
        List<InetSocketAddress> freezed = new LinkedList<>();
        for (ClientConnectionsEntry entry : entries) {
            if (entry.getClient().getConfig().getFailedNodeDetector().isNodeFailed()) {
                failed.add(entry.getClient().getAddr());
            } else if (entry.isFreezed()) {
                freezed.add(entry.getClient().getAddr());
            }
        }

        StringBuilder errorMsg = new StringBuilder(getClass().getSimpleName() + " no available Redis entries. Master entry host: " + masterSlaveEntry.getClient().getAddr());
        if (!freezed.isEmpty()) {
            errorMsg.append(" Disconnected hosts: ").append(freezed);
        }
        if (!failed.isEmpty()) {
            errorMsg.append(" Hosts disconnected due to errors during `failedSlaveCheckInterval`: ").append(failed);
        }

        RedisConnectionException exception = new RedisConnectionException(errorMsg.toString());
        CompletableFuture<T> result = new CompletableFuture<>();
        result.completeExceptionally(exception);
        return result;
    }

    public CompletableFuture<T> get(RedisCommand<?> command, ClientConnectionsEntry entry) {
        return acquireConnection(command, entry);
    }

    protected final CompletableFuture<T> acquireConnection(RedisCommand<?> command, ClientConnectionsEntry entry) {
        CompletableFuture<T> result = getConnectionHolder(entry).acquireConnection(command);
        result.whenComplete((r, e) -> {
            if (e != null) {
                if (entry.getNodeType() == NodeType.SLAVE) {
                    entry.getClient().getConfig().getFailedNodeDetector().onConnectFailed();
                    if (entry.getClient().getConfig().getFailedNodeDetector().isNodeFailed()) {
                        masterSlaveEntry.shutdownAndReconnectAsync(entry.getClient(), e);
                    }
                }
                return;
            }

            if (entry.getNodeType() == NodeType.SLAVE) {
                entry.getClient().getConfig().getFailedNodeDetector().onConnectSuccessful();
            }
        });
        return result;
    }
        
    private boolean isHealthy(ClientConnectionsEntry entry) {
        if (entry.getNodeType() == NodeType.SLAVE
                && entry.getClient().getConfig().getFailedNodeDetector().isNodeFailed()) {
            return false;
        }
        return true;
    }

    public final void returnConnection(ClientConnectionsEntry entry, T connection) {
        if (entry == null) {
            connection.closeAsync();
            return;
        }
        ConnectionsHolder<T> holder = getConnectionHolder(entry);
        if (entry.isFreezed() && entry.getFreezeReason() != FreezeReason.SYSTEM) {
            connection.closeAsync();
            holder.getAllConnections().remove(connection);
        } else {
            holder.releaseConnection(connection);
        }
        holder.releaseConnection();
    }

}
