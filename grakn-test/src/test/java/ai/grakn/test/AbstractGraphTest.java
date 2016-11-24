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

package ai.grakn.test;

import ai.grakn.GraknGraph;
import ai.grakn.GraknGraphFactory;
import org.junit.After;
import org.junit.Before;

/**
 * Abstract test class that provides a new empty graph every test that can be committed to.
 */
public abstract class AbstractGraphTest extends AbstractEngineTest {

    protected GraknGraphFactory factory;
    protected GraknGraph graph;

    @Before
    public void createGraph() {
        factory = factoryWithNewKeyspace();
        graph = factory.getGraph();
    }

    @After
    public void closeGraph() throws Exception {
        graph.close();
    }
}
