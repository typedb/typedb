/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.graph.vertex;

import com.vaticle.typedb.core.encoding.Encoding;

import java.time.LocalDateTime;

public interface Value<VALUE_TYPE> {

    Encoding.ValueType<VALUE_TYPE> valueType();

    VALUE_TYPE value();

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isDateTime();

    Value<Boolean> asBoolean();

    Value<Long> asLong();

    Value<Double> asDouble();

    Value<String> asString();

    Value<LocalDateTime> asDateTime();
}
