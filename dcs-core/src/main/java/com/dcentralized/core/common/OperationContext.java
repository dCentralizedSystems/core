/*
 * Copyright (c) 2014-2015 dCentralizedSystems, LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.dcentralized.core.common;

import com.dcentralized.core.common.Operation.AuthorizationContext;

/**
 * OperationContext encapsulates the runtime context of an Operation
 * The context is maintained as a thread local variable that is set
 * by the service host or the Operation object
 * OperationContext instances are immutable.
 */
public final class OperationContext {

    /**
     * Variable to store the OperationContext in thread-local
     */
    private static final ThreadLocal<OperationContext> threadOperationContext = ThreadLocal.withInitial(
            OperationContext::new);

    AuthorizationContext authContext;

    private OperationContext() {
    }

    static void setAuthorizationContext(AuthorizationContext ctx) {
        threadOperationContext.get().authContext = ctx;
    }

    static OperationContext get() {
        return threadOperationContext.get();
    }

    public static AuthorizationContext getAuthorizationContext() {
        return threadOperationContext.get().authContext;
    }

    /**
     * Set the OperationContext associated with the thread based on the specified OperationContext
     * @param opCtx Input OperationContext
     */
    public static void setFrom(OperationContext opCtx) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = opCtx.authContext;
    }

    /**
     * Set the OperationContext associated with the thread based on the specified Operation
     * @param op Operation to build the OperationContext
     */
    public static void setFrom(Operation op) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = op.getAuthorizationContext();
    }

    /**
     * reset the OperationContext associated with the thread
     */
    public static void reset() {
        OperationContext opCtx = threadOperationContext.get();
        opCtx.authContext = null;
    }

    /**
     * Restore the OperationContext associated with this thread to the value passed in
     * @param opCtx OperationContext instance to restore to
     */
    public static void restoreAuthContext(AuthorizationContext authContext) {
        OperationContext currentOpCtx = threadOperationContext.get();
        currentOpCtx.authContext = authContext;
    }
}
