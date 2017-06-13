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
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

//TODO: Move this test class to a lower dependency. Specifically Graql
public class GraphLoaderTest {

    @Test
    public void whenCreatingEmptyGraph_EnsureGraphIsEmpty(){
        GraphLoader loader = GraphLoader.empty();

        try (GraknGraph graph = loader.graph()){
            assertThat(graph.admin().getMetaEntityType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRelationType().instances(), is(empty()));
            assertThat(graph.admin().getMetaRuleType().instances(), is(empty()));
        }
    }
}
