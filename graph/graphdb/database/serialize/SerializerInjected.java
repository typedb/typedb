// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.core.attribute.AttributeSerializer;
import grakn.core.graph.diskstorage.WriteBuffer;

/**
 * Marks a {@link AttributeSerializer} that requires a {@link Serializer}
 * to serialize the internal state. It is expected that the serializer is passed into this object upon initialization and before usage.
 * Furthermore, such serializers will convert the {@link WriteBuffer} passed into the
 * {@link AttributeSerializer}'s write methods to be cast to {@link DataOutput}.
 */
public interface SerializerInjected {

    void setSerializer(Serializer serializer);

}
