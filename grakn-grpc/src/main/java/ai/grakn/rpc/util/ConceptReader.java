/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.rpc.util;

import ai.grakn.concept.AttributeType;
import ai.grakn.rpc.generated.GrpcConcept;

/**
 * A utility class to read RPC Concepts and convert them into Grakn Concepts.
 *
 * @author Grakn Warriors
 */
public class ConceptReader {
    public static Object attributeValue(GrpcConcept.AttributeValue value) {
        switch (value.getValueCase()) {
            case STRING:
                return value.getString();
            case BOOLEAN:
                return value.getBoolean();
            case INTEGER:
                return value.getInteger();
            case LONG:
                return value.getLong();
            case FLOAT:
                return value.getFloat();
            case DOUBLE:
                return value.getDouble();
            case DATE:
                return value.getDate();
            default:
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("Unrecognised " + value);
        }
    }

    public static AttributeType.DataType<?> dataType(GrpcConcept.DataType dataType) {
        switch (dataType) {
            case String:
                return AttributeType.DataType.STRING;
            case Boolean:
                return AttributeType.DataType.BOOLEAN;
            case Integer:
                return AttributeType.DataType.INTEGER;
            case Long:
                return AttributeType.DataType.LONG;
            case Float:
                return AttributeType.DataType.FLOAT;
            case Double:
                return AttributeType.DataType.DOUBLE;
            case Date:
                return AttributeType.DataType.DATE;
            default:
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Unrecognised " + dataType);
        }
    }
}
