/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concept.value;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.concept.Concept;
import com.vaticle.typedb.core.encoding.Encoding;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public interface Value<VALUE> extends Concept, Concept.Readable {

    DateTimeFormatter DATE_TIME_FORMATTER_MILLIS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

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
