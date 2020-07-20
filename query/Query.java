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

package grakn.core.query;

import grabl.tracing.client.GrablTracingThreadStatic;
import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.common.options.GraknOptions;
import grakn.core.concept.Concepts;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.graph.Graphs;
import graql.lang.Graql;
import graql.lang.query.GraqlQuery;

import java.util.stream.Stream;

import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;

public class Query {

    private final Graphs graphs;
    private final Concepts concepts;

    public Query(Graphs graphs, Concepts concepts) {
        this.concepts = concepts;
        this.graphs = graphs;
    }

    private GraqlQuery parse(String query) {
        try (ThreadTrace ignored2 = traceOnThread("parse")) {
            return Graql.parse(query);
        }
    }

    public Stream<ConceptMap> stream(String query, GraknOptions options) {
        try (ThreadTrace ignored2 = traceOnThread("stream")) {
            GraqlQuery graql = parse(query);

            return null; // TODO
        }
    }
}
