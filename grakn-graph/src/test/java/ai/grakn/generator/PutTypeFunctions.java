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
 *
 */

package ai.grakn.generator;

import ai.grakn.GraknGraph;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;
import com.google.common.collect.ImmutableList;

import java.util.function.BiFunction;

public class PutTypeFunctions extends AbstractGenerator<BiFunction> {

    public PutTypeFunctions() {
        super(BiFunction.class);
    }

    @Override
    protected BiFunction<GraknGraph, TypeLabel, Type> generate() {
        return random.choose(ImmutableList.of(
                GraknGraph::putEntityType,
                (graph, label) -> graph.putResourceType(label, gen(ResourceType.DataType.class)),
                GraknGraph::putRuleType,
                GraknGraph::putRelationType,
                GraknGraph::putRoleType
        ));
    }
}
