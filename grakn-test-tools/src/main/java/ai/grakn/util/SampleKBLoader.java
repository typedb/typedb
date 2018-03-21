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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.util;

import ai.grakn.Grakn;
import ai.grakn.GraknSystemProperty;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.engine.GraknConfig;
import ai.grakn.factory.EmbeddedGraknSession;
import ai.grakn.factory.TxFactoryBuilder;
import ai.grakn.factory.GraknSessionLocal;
import ai.grakn.factory.TxFactory;
import ai.grakn.graql.Query;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.kb.internal.GraknTxTinker;
import com.google.common.io.Files;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * <p>
 *     Builds {@link GraknTx} bypassing engine.
 * </p>
 *
 * <p>
 *     A helper class which is used to build {@link GraknTx} for testing purposes.
 *     This class bypasses requiring an instance of engine to be running in the background.
 *     Rather it acquires the necessary properties for building a graph directly from system properties.
 *     This does however mean that commit logs are not submitted and no post processing is ran
 * </p>
 *
 * @author fppt
 */
public class SampleKBLoader {

    private final TxFactory<?> factory;
    private @Nullable Consumer<GraknTx> preLoad;
    private boolean graphLoaded = false;
    private EmbeddedGraknTx<?> tx;

    private SampleKBLoader(@Nullable Consumer<GraknTx> preLoad){

        EmbeddedGraknSession session = GraknSessionLocal.create(randomKeyspace(), Grakn.IN_MEMORY, GraknConfig.create());
        factory = TxFactoryBuilder.getFactory(session, false);
        this.preLoad = preLoad;
    }

    public static SampleKBLoader empty(){
        return new SampleKBLoader(null);
    }

    public static SampleKBLoader preLoad(@Nullable Consumer<GraknTx> build){
        return new SampleKBLoader(build);
    }

    public EmbeddedGraknTx<?> tx(){
        if(tx == null || tx.isClosed()){
            //Load the graph if we need to
            if(!graphLoaded) {
                try(GraknTx graph = factory.open(GraknTxType.WRITE)){
                    load(graph);
                    graph.commit();
                    graphLoaded = true;
                }
            }

            tx = factory.open(GraknTxType.WRITE);
        }

        return tx;
    }

    public void rollback() {
        if (tx instanceof GraknTxTinker) {
            tx.admin().delete();
            graphLoaded = false;
        } else if (!tx.isClosed()) {
            tx.close();
        }
        tx = tx();
    }

    /**
     * Loads the graph using the specified Preloaders
     */
    private void load(GraknTx graph){
        if(preLoad != null) preLoad.accept(graph);
    }

    public static Keyspace randomKeyspace(){
        // Embedded Casandra has problems dropping keyspaces that start with a number
        return Keyspace.of("a"+ UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public static void loadFromFile(GraknTx graph, String file) {
        File graql = new File(GraknSystemProperty.PROJECT_RELATIVE_DIR.value() + "/grakn-test-tools/src/main/graql/" + file);

        List<String> queries;
        try {
            queries = Files.readLines(graql, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        graph.graql().parser().parseList(queries.stream().collect(Collectors.joining("\n"))).forEach(Query::execute);
    }
}
