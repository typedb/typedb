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

package ai.grakn.migration.json;

import ai.grakn.GraknGraph;
import ai.grakn.concept.*;
import ai.grakn.graql.internal.util.GraqlType;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;

public class JsonMigratorUtil {

    public static File getFile(String fileName){
        return new File(JsonMigratorUtil.class.getClassLoader().getResource(fileName).getPath());
    }

    public static Instance getProperty(GraknGraph graph, Instance instance, String name) {
        assertEquals(1, getProperties(graph, instance, name).size());
        return getProperties(graph, instance, name).iterator().next();
    }

    public static Collection<Instance> getProperties(GraknGraph graph, Instance instance, String name) {
        RelationType relation = graph.getRelationType(name);

        Set<Instance> instances = new HashSet<>();

        relation.instances().stream()
                .filter(i -> i.rolePlayers().values().contains(instance))
                .forEach(i -> instances.addAll(i.rolePlayers().values()));

        instances.remove(instance);
        return instances;
    }

    public static Resource getResource(GraknGraph graph, Instance instance, String name) {
        assertEquals(1, getResources(graph, instance, name).count());
        return getResources(graph, instance, name).findAny().get();
    }

    public static Stream<Resource> getResources(GraknGraph graph, Instance instance, String name) {
        RoleType roleOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(name));
        RoleType roleOther = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(name));

        Collection<Relation> relations = instance.relations(roleOwner);
        return relations.stream().map(r -> r.rolePlayers().get(roleOther).asResource());
    }
}
