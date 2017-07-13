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

package ai.grakn.factory;

import ai.grakn.GraknTxType;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class TitanHadoopInternalFactoryTest {
    private final static Properties TEST_PROPERTIES = new Properties();

    private TitanHadoopInternalFactory factory;

    @Before
    public void setUp() throws Exception {
        String TEST_CONFIG = "../../conf/main/grakn.properties";
        try (InputStream in = new FileInputStream(TEST_CONFIG)){
            TEST_PROPERTIES.load(in);
        } catch (IOException e) {
            throw new RuntimeException(ErrorMessage.INVALID_PATH_TO_CONFIG.getMessage(TEST_CONFIG), e);
        }

        factory = new TitanHadoopInternalFactory("rubbish", "rubbish", TEST_PROPERTIES);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void buildGraknGraphFromTinker() throws Exception {
        factory.open(GraknTxType.WRITE);
    }

    @Test
    public void buildTinkerPopGraph() throws Exception {
        assertThat(factory.getTinkerPopGraph(false), instanceOf(HadoopGraph.class));
    }

    @Test
    public void testSingleton(){
        HadoopGraph graph1 = factory.getTinkerPopGraph(false);
        HadoopGraph graph2 = factory.getTinkerPopGraph(false);

        assertEquals(graph1, graph2);
    }

}