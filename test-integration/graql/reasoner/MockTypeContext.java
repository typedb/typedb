/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graql.reasoner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.reasoner.operator.TypeContext;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class MockTypeContext implements TypeContext {

    private final Map<String, String> sups = ImmutableMap.<String, String>builder()
            .put("baseEntity", "entity")
            .put("baseRelation", "relation")
            .put("subEntity","baseEntity")
            .put("subRelation", "baseRelation")
            .put("baseRole", "role")
            .put("subRole", "baseRole")
            .build();

    private final Set<String> metaTypes = ImmutableSet.<String>builder()
            .add("role").add("thing")
            .add("entity").add("relation")
            .build();

    @Override
    public boolean isMetaType(String label) {
        return metaTypes.contains(label);
    }

    @Override
    public String sup(String label) {
        return sups.get(label);
    }

    @Override
    public Stream<String> sups(String label) {
        return Stream.empty();
    }

    @Override
    public Stream<String> subs(String label) {
        return Stream.empty();
    }
}
