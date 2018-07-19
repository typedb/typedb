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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.answer.ConceptMap;

import java.util.stream.Stream;

/**
 * Aggregate that checks if there are any results
 *
 * @author Felix Chapman
 */
public class AskAggregate extends AbstractAggregate<Boolean> {

    private static final AskAggregate INSTANCE = new AskAggregate();

    private AskAggregate() { }

    // This class has no parameters and is immutable, so a singleton is good practice
    public static AskAggregate get() {
        return INSTANCE;
    }

    @Override
    public Boolean apply(Stream<? extends ConceptMap> stream) {
        return stream.findAny().isPresent();
    }

    @Override
    public String toString() {
        return "ask";
    }
}
