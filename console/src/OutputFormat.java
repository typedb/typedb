/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.console;

import grakn.core.concept.AttributeType;
import grakn.core.graql.internal.printer.Printer;

import java.util.Set;

/**
 * Valid output formats for the Graql shell
 *
 */
public abstract class OutputFormat {

    abstract Printer<?> getPrinter(Set<AttributeType<?>> displayAttributes);

    static final OutputFormat DEFAULT = new OutputFormat.Graql();

    static class Graql extends OutputFormat {
        @Override
        Printer<?> getPrinter(Set<AttributeType<?>> displayAttributes) {
            AttributeType<?>[] array = displayAttributes.toArray(new AttributeType[displayAttributes.size()]);
            return Printer.stringPrinter(true, array);
        }
    }
}
