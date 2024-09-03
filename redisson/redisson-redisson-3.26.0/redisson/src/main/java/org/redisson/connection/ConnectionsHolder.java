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
package org.redisson.connection;

import org.redisson.client.RedisClient;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.misc.AsyncSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class ConnectionsHolder<T extends RedisConnection> {

    final Logger log = LoggerFactory.getLogger(getClass());

    private final Queue<T> allConnections = new ConcurrentLinkedQueue<>();
    private final Queue<T> freeConnections = new ConcurrentLinkedQueue<>();
    private final AsyncSemaphore freeConnectionsCounter;

    private final RedisClient client;

    private final Function<RedisClient, CompletionStage<T>> connectionCallback;

    private final ServiceManager serviceManager;

    private final boolean changeUsage;

    public ConnectionsHolder(RedisClient client, int poolMaxSize,
                             Function<RedisClient, CompletionStage<T>> connectionCallback,
                             ServiceManager serviceManager, boolean changeUsage) {
        this.freeConnectionsCounter = new AsyncSemaphore(poolMaxSize);
        this.client = client;
        this.connectionCallback = connectionCallback;
        this.serviceManager = serviceManager;
        this.changeUsage = changeUsage;
    }

    public <R extends RedisConnection> boolean remove(R connection) {
        if (freeConnections.remove(connection)) {
            return allConnections.remove(connection);
        }
        return false;
    }

    public Queue<T> getFreeConnections() {
        return freeConnections;
    }

    public AsyncSemaphore getFreeConnectionsCounter() {
        return freeConnectionsCounter;
    }

    public CompletableFuture<Void> acquireConnection() {
        return freeConnectionsCounter.acquire();
    }
    
    public void releaseConnection() {
        freeConnectionsCounter.release();
    }

    public void addConnection(T conn) {
        conn.setLastUsageTime(System.nanoTime());
        freeConnections.add(conn);
    }

    public T pollConnection(RedisCommand<?> command) {
        T c = freeConnections.poll();
        if (c != null) {
            c.incUsage();
        }
        return c;
    }

    public void releaseConnection(T connection) {
        if (connection.isClosed()) {
            return;
        }

        if (client != connection.getRedisClient()) {
            connection.closeAsync();
            return;
        }

        connection.setLastUsageTime(System.nanoTime());
        freeConnections.add(connection);
        connection.decUsage();
    }

    public CompletionStage<T> connect() {
        CompletionStage<T> future = connectionCallback.apply(client);
        return future.whenComplete((conn, e) -> {
            if (e != null) {
                return;
            }

            log.debug("new connection created: {}", conn);
            
            allConnections.add(conn);
        });
    }

    public Queue<T> getAllConnections() {
        return allConnections;
    }

    public CompletableFuture<Void> initConnections(int minimumIdleSize) {
        if (minimumIdleSize == 0) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> initPromise = new CompletableFuture<>();
        AtomicInteger initializedConnections = new AtomicInteger(minimumIdleSize);
        createConnection(initPromise, minimumIdleSize, initializedConnections);
        return initPromise;
    }

    private void createConnection(CompletableFuture<Void> initPromise, int minimumIdleSize, AtomicInteger initializedConnections) {

        CompletableFuture<Void> f = acquireConnection();
        f.thenAccept(r -> {
            CompletableFuture<T> promise = new CompletableFuture<>();
            createConnection(promise);
            promise.whenComplete((conn, e) -> {
                if (e == null) {
                    if (changeUsage) {
                        conn.decUsage();
                    }
                    if (!initPromise.isDone()) {
                        addConnection(conn);
                    } else {
                        conn.closeAsync();
                    }
                }

                releaseConnection();

                if (e != null) {
                    if (initPromise.isDone()) {
                        return;
                    }

                    for (RedisConnection connection : getAllConnections()) {
                        if (!connection.isClosed()) {
                            connection.closeAsync();
                        }
                    }
                    getAllConnections().clear();

                    int totalInitializedConnections = minimumIdleSize - initializedConnections.get();
                    String errorMsg;
                    if (totalInitializedConnections == 0) {
                        errorMsg = "Unable to connect to Redis server: " + client.getAddr();
                    } else {
                        errorMsg = "Unable to init enough connections amount! Only " + totalInitializedConnections
                                + " of " + minimumIdleSize + " were initialized. Redis server: " + client.getAddr();
                    }
                    Exception cause = new RedisConnectionException(errorMsg, e);
                    initPromise.completeExceptionally(cause);
                    return;
                }

                int value = initializedConnections.decrementAndGet();
                if (value == 0) {
                    if (initPromise.complete(null)) {
                        log.info("{} connections initialized for {}", minimumIdleSize, client.getAddr());
                    }
                } else if (value > 0 && !initPromise.isDone()) {
                    createConnection(initPromise, minimumIdleSize, initializedConnections);
                }
            });
        });
    }

    private void createConnection(CompletableFuture<T> promise) {
        CompletionStage<T> connFuture = connect();
        connFuture.whenComplete((conn, e) -> {
            if (e != null) {
                promiseFailure(promise, e);
                return;
            }

            if (changeUsage) {
                promise.thenApply(c -> c.incUsage());
            }
            connectedSuccessful(promise, conn);
        });
    }

    private void promiseFailure(CompletableFuture<T> promise, Throwable cause) {
        releaseConnection();

        promise.completeExceptionally(cause);
    }

    private void connectedSuccessful(CompletableFuture<T> promise, T conn) {
        if (!promise.complete(conn)) {
            releaseConnection(conn);
            releaseConnection();
        }
    }

    public CompletableFuture<T> acquireConnection(RedisCommand<?> command) {
        CompletableFuture<T> result = new CompletableFuture<>();

        CompletableFuture<Void> f = acquireConnection();
        f.thenAccept(r -> {
            connectTo(result, command);
        });
        result.whenComplete((r, e) -> {
            if (e != null) {
                f.completeExceptionally(e);
            }
        });
        return result;
    }

    private void connectTo(CompletableFuture<T> promise, RedisCommand<?> command) {
        if (promise.isDone()) {
            serviceManager.getGroup().submit(() -> {
                releaseConnection();
            });
            return;
        }

        T conn = pollConnection(command);
        if (conn != null) {
            connectedSuccessful(promise, conn);
            return;
        }

        createConnection(promise);
    }

    @Override
    public String toString() {
        return "ConnectionsHolder{" +
                "allConnections=" + allConnections.size() +
                ", freeConnections=" + freeConnections.size() +
                ", freeConnectionsCounter=" + freeConnectionsCounter +
                '}';
    }
}

