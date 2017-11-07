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

package com.dcentralized.core.services.common;

import com.dcentralized.core.common.FactoryService;
import com.dcentralized.core.common.Operation;
import com.dcentralized.core.common.Service;
import com.dcentralized.core.common.ServiceDocument;
import com.dcentralized.core.common.ServiceHost;
import com.dcentralized.core.common.UriUtils;
import com.dcentralized.core.services.common.NodeGroupService.NodeGroupState;

/**
 * Factory service for creating node groups
 */
public class NodeGroupFactoryService extends FactoryService {
    public static final String SELF_LINK = ServiceUriPaths.NODE_GROUP_FACTORY;

    public NodeGroupFactoryService() {
        super(NodeGroupState.class);
    }

    @Override
    public Service createServiceInstance() throws Throwable {
        return new NodeGroupService();
    }

    public static Operation createNodeGroupPostOp(
            ServiceHost host, String groupName) {
        ServiceDocument d = new ServiceDocument();
        d.documentSelfLink = groupName;
        return Operation
                .createPost(UriUtils.buildUri(host, NodeGroupFactoryService.class))
                .setBody(d);
    }

}
