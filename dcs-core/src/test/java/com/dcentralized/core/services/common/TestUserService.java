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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashSet;
import java.util.UUID;

import com.dcentralized.core.common.BasicReusableHostTestCase;
import com.dcentralized.core.common.Operation;
import com.dcentralized.core.common.Service.Action;
import com.dcentralized.core.common.ServiceErrorResponse;
import com.dcentralized.core.common.UriUtils;
import com.dcentralized.core.common.test.TestRequestSender;
import com.dcentralized.core.common.test.TestRequestSender.FailureResponse;
import com.dcentralized.core.services.common.UserService.UserState;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestUserService extends BasicReusableHostTestCase {
    private TestRequestSender sender;

    @Before
    public void setUp() {
        this.sender = new TestRequestSender(this.host);
    }

    @After
    public void cleanUp() throws Throwable {
        URI factoryUri = UriUtils.buildUri(this.host, ServiceUriPaths.CORE_AUTHZ_USERS);
        this.host.deleteAllChildServices(factoryUri);
    }

    @Test
    public void testFactoryPostAndDelete() {
        UserState state = new UserState();
        state.email = "jane@doe.com";

        Operation op = Operation.createPost(this.host, ServiceUriPaths.CORE_AUTHZ_USERS).setBody(state);
        UserState outState = this.sender.sendAndWait(op, UserState.class);

        assertEquals(state.email, outState.email);
    }


    @Test
    public void testFactoryIdempotentPost() throws Throwable {
        String email = "jane@doe.com";
        String servicePath = UriUtils.buildUriPath(UserService.FACTORY_LINK, email);

        UserState state = new UserState();
        state.email = email;
        state.documentSelfLink = servicePath;

        UserState responseState = this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email,responseState.email);

        long initialVersion = responseState.documentVersion;

        // sending same document, this post/put should not persist(increment) the document
        responseState = this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email,responseState.email);

        UserState getState = this.sender.sendAndWait(Operation.createGet(this.host, servicePath), UserState.class);
        assertEquals("version should not increase", initialVersion, getState.documentVersion);


        state.email = "john@doe.com";

        responseState = this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email, responseState.email);
        assertTrue("version should increase", initialVersion < responseState.documentVersion);
    }

    @Test
    public void testFactoryPostFailure() {
        UserState state = new UserState();
        state.email = "not an email";

        Operation op = Operation.createPost(this.host, ServiceUriPaths.CORE_AUTHZ_USERS).setBody(state);
        FailureResponse response = this.sender.sendAndWaitFailure(op);

        assertEquals(Operation.STATUS_CODE_FAILURE_THRESHOLD, response.op.getStatusCode());
        assertEquals("email is invalid", response.failure.getMessage());
    }

    @Test
    public void testPatch() throws Throwable {
        UserState state = new UserState();
        state.email = "jane@doe.com";
        state.documentSelfLink = UUID.randomUUID().toString();
        state.userGroupLinks = new HashSet<String>();
        state.userGroupLinks.add("link1");
        state.userGroupLinks.add("link2");


        UserState responseState = this.host.verifyPost(UserState.class,
                ServiceUriPaths.CORE_AUTHZ_USERS,
                state,
                Operation.STATUS_CODE_OK);

        assertEquals(state.email, responseState.email);
        assertEquals(state.userGroupLinks.size(), state.userGroupLinks.size());

        state.email = "john@doe.com";
        state.userGroupLinks.clear();
        state.userGroupLinks.add("link2");
        state.userGroupLinks.add("link3");

        String path = UriUtils.buildUriPath(ServiceUriPaths.CORE_AUTHZ_USERS, state.documentSelfLink);
        Operation op = Operation.createPatch(this.host, path).setBody(state);

        UserState patchedState = this.sender.sendAndWait(op, UserState.class);
        assertEquals(state.email, patchedState.email);
        assertEquals(3, patchedState.userGroupLinks.size());
    }

    @Test
    public void testGetBeforePost() throws Throwable {
        String email = "jake@doe.com";
        String servicePath = UriUtils.buildUriPath(UserService.FACTORY_LINK, email);

        // get a non-existed doc
        FailureResponse f = this.sender.sendAndWaitFailure(Operation.createGet(this.host, servicePath));
        assertEquals(Operation.STATUS_CODE_NOT_FOUND,  f.op.getStatusCode());

        UserState state = new UserState();
        state.email = email;
        state.documentSelfLink = servicePath;

        // POST operation would have failed on ServiceNotFoundException, now exception should be gone
        Operation op = Operation.createPost(this.host, UserService.FACTORY_LINK).setBody(state);
        UserState outState = this.sender.sendAndWait(op, UserState.class);

        assertEquals(state.email, outState.email);
        assertEquals(servicePath, outState.documentSelfLink);
    }

    @Test
    // this test is to make sure current change does not change original behavior of "DELETE then POST"
    public void testDeleteThenGetThenPost() throws Throwable {
        String email = "july@doe.com";
        String servicePath = UriUtils.buildUriPath(UserService.FACTORY_LINK, email);

        UserState state = new UserState();
        state.email = email;
        state.documentSelfLink = servicePath;

        Operation op = Operation.createPost(this.host, UserService.FACTORY_LINK).setBody(state);
        this.sender.sendAndWait(op, UserState.class);

        Operation get = Operation.createGet(this.host, servicePath);
        UserState outState  = this.sender.sendAndWait(get, UserState.class);

        assertEquals(state.email, outState.email);
        assertEquals(servicePath, outState.documentSelfLink);

        Operation delete = Operation.createDelete(this.host, servicePath);
        outState = this.sender.sendAndWait(delete, UserState.class);
        assertEquals(Action.DELETE.toString(), outState.documentUpdateAction);

        FailureResponse f = this.sender.sendAndWaitFailure(Operation.createGet(this.host, servicePath));
        assertEquals(Operation.STATUS_CODE_NOT_FOUND,  f.op.getStatusCode());

        state = new UserState();
        state.email = email;
        state.documentSelfLink = servicePath;

        op = Operation.createPost(this.host, UserService.FACTORY_LINK).setBody(state);
        f = this.sender.sendAndWaitFailure(op);
        assertEquals(Operation.STATUS_CODE_CONFLICT, f.op.getStatusCode());
        ServiceErrorResponse r = (ServiceErrorResponse)f.op.getBodyRaw();
        assertEquals(ServiceErrorResponse.ERROR_CODE_STATE_MARKED_DELETED,r.getErrorCode());
    }
}
