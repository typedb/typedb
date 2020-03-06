/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.kb.graql.executor;

import grakn.core.concept.answer.ConceptList;
import grakn.core.concept.answer.ConceptSet;
import grakn.core.concept.answer.ConceptSetMeasure;
import grakn.core.concept.answer.Numeric;
import grakn.core.kb.concept.api.LabelId;
import graql.lang.query.GraqlCompute;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Execute Compute queries
 */
public interface ComputeExecutor {
    Stream<Numeric> stream(GraqlCompute.Statistics query);

    Stream<ConceptList> stream(GraqlCompute.Path query);

    Stream<ConceptSetMeasure> stream(GraqlCompute.Centrality query);

    Stream<ConceptSet> stream(GraqlCompute.Cluster query);

    ComputerResult compute(@Nullable VertexProgram<?> program,
                           @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                           @Nullable Set<LabelId> scope,
                           Boolean includesRolePlayerEdges);

    ComputerResult compute(@Nullable VertexProgram<?> program,
                           @Nullable MapReduce<?, ?, ?, ?, ?> mapReduce,
                           @Nullable Set<LabelId> scope);
}
