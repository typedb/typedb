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

package grakn.core.graql.query.aggregate;

import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.query.Aggregate;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An aggregate that changes match clause results into a list.
 */
public class ListAggregate implements Aggregate<ConceptMap> {

    @Override
    public List<ConceptMap> apply(Stream<? extends ConceptMap> stream) {
        return stream.collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return 31;
    }
}
