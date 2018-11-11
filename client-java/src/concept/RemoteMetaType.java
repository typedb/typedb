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

package grakn.core.client.concept;

import grakn.core.client.Grakn;
import grakn.core.concept.Concept;
import grakn.core.concept.ConceptId;
import grakn.core.concept.Thing;
import grakn.core.concept.Type;
import com.google.auto.value.AutoValue;

/**
 * Client implementation of {@link grakn.core.concept.Type}
 */
@AutoValue
public abstract class RemoteMetaType extends RemoteType<Type, Thing> {

    static RemoteMetaType construct(Grakn.Transaction tx, ConceptId id) {
        return new AutoValue_RemoteMetaType(tx, id);
    }

    @Override
    final Type asCurrentBaseType(Concept other) {
        return other.asType();
    }

    @Override
    boolean equalsCurrentBaseType(Concept other) {
        return other.isType();
    }

    @Override
    protected final Thing asInstance(Concept concept) {
        return concept.asThing();
    }
}
