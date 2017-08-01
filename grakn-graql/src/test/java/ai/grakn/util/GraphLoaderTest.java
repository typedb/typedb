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
 */
package ai.grakn.util;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.test.GraknTestSetup;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class GraphLoaderTest {

    //TODO: Put this somewhere common
    @Before
    public void setup(){
        GraknTestSetup.startCassandraIfNeeded();
    }

    @Test
    public void whenCreatingEmptyGraph_EnsureGraphIsEmpty(){
        GraphLoader loader = GraphLoader.empty();

        try (GraknGraph graph = loader.graph()){
            assertThat(graph.admin().getMetaEntityType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRelationType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRuleType().instances(), is(empty()));
        }
    }

    @Test
    public void whenCreatingGraphWithPreLoader_EnsureGraphContainsPreLoadedEntities(){
        Set<Label> labels = new HashSet<>(Arrays.asList(Label.of("1"), Label.of("2"), Label.of("3")));

        Consumer<GraknGraph> preLoader = graph -> labels.forEach(graph::putEntityType);

        GraphLoader loader = GraphLoader.preLoad(preLoader);

        try (GraknGraph graph = loader.graph()){
            Set<Label> foundLabels = graph.admin().getMetaEntityType().subs().stream().
                    map(Type::getLabel).collect(Collectors.toSet());

            assertTrue(foundLabels.containsAll(labels));
        }
    }

    @Test
    public void whenBuildingGraph_EnsureBackendMatchesTheTestProfile(){
        try(GraknGraph graph = GraphLoader.empty().graph()){
            //String comparison is used here because we do not have the class available at compile time
            if(GraknTestSetup.usingTinker()){
                assertEquals("ai.grakn.graph.internal.GraknTinkerGraph", graph.getClass().getName());
            } else if (GraknTestSetup.usingJanus()) {
                assertEquals("ai.grakn.graph.internal.GraknJanusGraph", graph.getClass().getName());
            } else {
                throw new RuntimeException("Test run with unsupported graph backend");
            }
        }
    }

    @Test
    public void whenCreatingGraphWithPreLoadingFiles_EnsureGraphContainsPreLoadedEntities(){
        //Create some rubbish files
        String [] files = new String[10];
        String [] typeNames = new String[10];
        try {
            for(int i = 0; i < files.length; i ++) {
                File temp = File.createTempFile("some-graql-file-" + i, ".gql");
                temp.deleteOnExit();

                try(BufferedWriter out = new BufferedWriter(new FileWriter(temp))){
                    typeNames[i]= "my-entity-type-" + i;
                    out.write("insert " + typeNames[i] + " sub entity;");
                }

                files[i] = temp.getAbsolutePath();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //Load the data in
        GraphLoader loader = GraphLoader.preLoad(files);

        //Check the data is there
        try (GraknGraph graph = loader.graph()){
            for (String typeName : typeNames) {
                assertNotNull(graph.getEntityType(typeName));
            }
        }
    }
}
