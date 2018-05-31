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

package ai.grakn.graql.shell;

import ai.grakn.concept.AttributeType;
import ai.grakn.graql.internal.printer.Printer;

import java.util.Set;

/**
 * Valid output formats for the Graql shell
 *
 * @author Grakn Warriors
 */
public abstract class OutputFormat {

    abstract Printer<?> getPrinter(Set<AttributeType<?>> displayAttributes);

    static final OutputFormat DEFAULT = new OutputFormat.Graql();
    
    public static OutputFormat get(String name) {
        switch (name) {
            case "json":
                return new OutputFormat.JSON();
            case "graql":
            default:
                return new OutputFormat.Graql();
        }
    }

    static class JSON extends OutputFormat {
        @Override
        Printer<?> getPrinter(Set<AttributeType<?>> displayAttributes) {
            return Printer.jsonPrinter();
        }
    }

    static class Graql extends OutputFormat {
        @Override
        Printer<?> getPrinter(Set<AttributeType<?>> displayAttributes) {
            AttributeType<?>[] array = displayAttributes.toArray(new AttributeType[displayAttributes.size()]);
            return Printer.stringPrinter(true, array);
        }
    }
}
