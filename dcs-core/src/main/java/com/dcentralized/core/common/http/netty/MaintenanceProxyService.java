/*
 * Copyright (c) 2014-2016 dCentralizedSystems, LLC. All Rights Reserved.
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

package com.dcentralized.core.common.http.netty;

import java.util.function.Consumer;

import com.dcentralized.core.common.Operation;
import com.dcentralized.core.common.ServiceHost;
import com.dcentralized.core.common.StatelessService;
import com.dcentralized.core.common.UriUtils;
import com.dcentralized.core.services.common.ServiceUriPaths;

public class MaintenanceProxyService extends StatelessService {

    public static void start(ServiceHost host, Consumer<Operation> parentHandler) {
        MaintenanceProxyService s = new MaintenanceProxyService(parentHandler);
        String path = UriUtils.buildUriPath(ServiceUriPaths.CORE, "netty-maint-proxies",
                host.nextUUID());
        host.startService(Operation.createPost(UriUtils.buildUri(host, path)), s);
    }

    private Consumer<Operation> parentHandler;

    public MaintenanceProxyService(Consumer<Operation> parentHandler) {
        super.toggleOption(ServiceOption.CORE, true);
        super.toggleOption(ServiceOption.PERIODIC_MAINTENANCE, true);
        this.parentHandler = parentHandler;
    }

    @Override
    public void handleMaintenance(Operation post) {
        this.parentHandler.accept(post);
    }
}