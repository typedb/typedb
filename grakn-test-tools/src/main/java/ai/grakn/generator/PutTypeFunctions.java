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

package ai.grakn.generator;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import com.google.common.collect.ImmutableList;

import java.util.function.BiFunction;

/**
 * Generator that produces {@link GraknTx} methods that put a type in the graph, given {@link Label}.
 *
 * @author Felix Chapman
 */
public class PutTypeFunctions extends AbstractGenerator<BiFunction> {

    public PutTypeFunctions() {
        super(BiFunction.class);
    }

    @Override
    protected BiFunction<GraknTx, Label, Type> generate() {
        return random.choose(ImmutableList.of(
                GraknTx::putEntityType,
                (graph, label) -> graph.putAttributeType(label, gen(AttributeType.DataType.class)),
                GraknTx::putRelationshipType
        ));
    }
}
