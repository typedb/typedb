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
package ai.grakn.util;

import ai.grakn.GraknTx;
import ai.grakn.concept.Label;
import ai.grakn.concept.Type;
import ai.grakn.kb.internal.GraknTxTinker;
import ai.grakn.test.rule.SessionContext;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SampleKBLoaderTest {

    @ClassRule
    public static SessionContext sessionContext = SessionContext.create();

    @Test
    public void whenCreatingEmptyGraph_EnsureGraphIsEmpty(){
        SampleKBLoader loader = SampleKBLoader.empty();

        try (GraknTx graph = loader.tx()){
            assertThat(graph.admin().getMetaEntityType().instances().collect(toSet()), is(empty()));
            assertThat(graph.admin().getMetaRelationType().instances().collect(toSet()), is(empty()));
        }
    }

    @Test
    public void whenCreatingGraphWithPreLoader_EnsureGraphContainsPreLoadedEntities(){
        Set<Label> labels = new HashSet<>(Arrays.asList(Label.of("1"), Label.of("2"), Label.of("3")));

        Consumer<GraknTx> preLoader = graph -> labels.forEach(graph::putEntityType);

        SampleKBLoader loader = SampleKBLoader.preLoad(preLoader);

        try (GraknTx graph = loader.tx()){
            Set<Label> foundLabels = graph.admin().getMetaEntityType().subs().
                    map(Type::label).collect(Collectors.toSet());

            assertTrue(foundLabels.containsAll(labels));
        }
    }

    @Test
    public void whenBuildingGraph_EnsureBackendMatchesTheTestProfile(){
        try(GraknTx graph = SampleKBLoader.empty().tx()){
            //String comparison is used here because we do not have the class available at compile time
            if(GraknTestUtil.usingTinker()){
                assertEquals(GraknTxTinker.class.getSimpleName(), graph.getClass().getSimpleName());
            } else if (GraknTestUtil.usingJanus()) {
                assertEquals("GraknTxJanus", graph.getClass().getSimpleName());
            } else {
                throw new RuntimeException("Test run with unsupported graph backend");
            }
        }
    }
}
