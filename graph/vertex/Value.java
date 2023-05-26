/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
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
