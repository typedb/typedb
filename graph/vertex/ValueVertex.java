/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.encoding.Encoding;
import com.vaticle.typedb.core.encoding.iid.VertexIID;

import java.time.LocalDateTime;

public interface ValueVertex<VALUE_TYPE> extends Vertex<VertexIID.Value<VALUE_TYPE>, Encoding.Vertex.Value>, Value<VALUE_TYPE> {

    ValueVertex<Boolean> asBoolean();

    ValueVertex<Long> asLong();

    ValueVertex<Double> asDouble();

    ValueVertex<String> asString();

    ValueVertex<LocalDateTime> asDateTime();

}
