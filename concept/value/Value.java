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

package com.vaticle.typedb.core.concept.value;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.encoding.Encoding;

import java.time.LocalDateTime;

public interface Value<VALUE> extends Concept, Concept.Readable {

    ByteArray getIID();

    VALUE value();

    Encoding.ValueType<VALUE> valueType();

    boolean isBoolean();

    boolean isLong();

    boolean isDouble();

    boolean isString();

    boolean isDateTime();

    Boolean asBoolean();

    Long asLong();

    Double asDouble();

    String asString();

    DateTime asDateTime();

    interface Boolean extends Value<java.lang.Boolean> {

        java.lang.Boolean value();
    }

    interface Long extends Value<java.lang.Long> {

        java.lang.Long value();
    }

    interface Double extends Value<java.lang.Double> {

        java.lang.Double value();
    }

    interface String extends Value<java.lang.String> {

        java.lang.String value();
    }

    interface DateTime extends Value<LocalDateTime> {

        LocalDateTime value();
    }
}
