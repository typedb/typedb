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

package grakn.core.concept.exception;

import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;

public class GraknConceptException extends GraknException {

    GraknConceptException(String error) {
        super(error);
    }

    protected GraknConceptException(String error, Exception e) {
        super(error, e);
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    public static GraknConceptException create(String error) {
        return new GraknConceptException(error);
    }

    /**
     * Thrown when casting Grakn concepts/answers incorrectly.
     */
    public static GraknConceptException invalidCasting(Object concept, Class type) {
        return GraknConceptException.create(ErrorMessage.INVALID_OBJECT_TYPE.getMessage(concept, type));
    }
}
