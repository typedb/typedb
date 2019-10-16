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

package grakn.core.kb.server;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Set;

public interface TransactionAnalytics {
    @CheckReturnValue
    ComputerResult compute(@Nullable VertexProgram program, @Nullable MapReduce mapReduce,
                           @Nullable Set<grakn.core.kb.concept.api.LabelId> types, Boolean includesRolePlayerEdges);

    @CheckReturnValue
    ComputerResult compute(@Nullable VertexProgram program, @Nullable MapReduce mapReduce,
                           @Nullable Set<grakn.core.kb.concept.api.LabelId> types);

    void killJobs();
}
