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

package grakn.core.client.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

public class GraknClientException extends GraknException {

    GraknClientException(String error) {
        super(error);
    }

    protected GraknClientException(String error, Exception e) {
        super(error, e);
    }

    public static GraknClientException create(String error) {
        return new GraknClientException(error);
    }

    public static GraknClientException invalidKeyspaceName(String keyspace) {
        return create(ErrorMessage.INVALID_KEYSPACE_NAME.getMessage(keyspace));
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }
}
