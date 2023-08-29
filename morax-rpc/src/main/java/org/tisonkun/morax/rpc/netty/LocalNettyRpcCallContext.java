/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tisonkun.morax.rpc.netty;

import java.util.concurrent.CompletableFuture;
import org.tisonkun.morax.rpc.RpcAddress;

/**
 * If the sender and the receiver are in the same process, the reply can be sent back via local future.
 */
public class LocalNettyRpcCallContext extends NettyRpcCallContext {
    private final CompletableFuture<Object> future;

    public LocalNettyRpcCallContext(RpcAddress senderAddress, CompletableFuture<Object> future) {
        super(senderAddress);
        this.future = future;
    }

    @Override
    protected void send(Object message) {
        future.complete(message);
    }
}
