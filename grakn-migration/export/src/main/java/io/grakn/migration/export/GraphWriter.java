/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.migration.export;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public class GraphWriter {

    private static final String EOL = ";\n";
    private static final String INSERT = "insert\n";

    private final GraknGraph graph;
    private final List<String> reserved = Arrays.asList("inference-rule", "constraint-rule");

    public GraphWriter(GraknGraph graph){
        this.graph = graph;
    }

    public String dumpOntology(){
        return joinAsInsert(types().map(TypeMapper::map));
    }

    public String dumpData(){
        return joinAsInsert(types()
                .filter(t -> t.superType() == null)
                .filter(t -> !t.isRoleType())
                .flatMap(c -> c.instances().stream())
                .map(Concept::asInstance)
                .map(InstanceMapper::map));
    }

    private String joinAsInsert(Stream<String> stream){
        return stream.filter(s -> !s.isEmpty())
                .collect(joining(EOL, INSERT, EOL));
    }

    /**
     * @return a stream of all types with non-reserved IDs
     */
    private Stream<Type> types(){
        return graph.getMetaType().instances().stream()
                .map(Concept::asType)
                .filter(t -> !reserved.contains(t.getId()));
    }
}
