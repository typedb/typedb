/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Printer;
import mjson.Json;

import java.util.function.Function;

/**
 * Factory method for {@link Printer} implementations, for printing Graql results in multiple ways.
 *
 * @author Felix Chapman
 */
public class Printers {

    private Printers() {}

    public static Printer<Function<StringBuilder, StringBuilder>> graql(ResourceType... resourceTypes) {
        return new GraqlPrinter(resourceTypes);
    }

    public static Printer<Json> json() {
        return new JsonPrinter();
    }

    public static Printer hal() {
        return new HALPrinter();
    }
}
