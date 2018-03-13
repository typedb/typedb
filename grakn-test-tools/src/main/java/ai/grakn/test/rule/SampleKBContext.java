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

package ai.grakn.test.rule;

import ai.grakn.GraknTx;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.util.SampleKBLoader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.junit.rules.TestRule;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

/**
 * <p>
 *     Sets up graphs for testing
 * </p>
 *
 * <p>
 *     Contains utility methods and statically initialized environment variables to control
 *     Grakn unit tests.
 *
 *     This specific class extend {@link SampleKBLoader} and starts Cassandra instance via
 *     {@link EmbeddedCassandraContext} if needed.
 * </p>
 *
 * @author borislav, fppt
 *
 */
public class SampleKBContext extends CompositeTestRule {
    private final SampleKBLoader loader;

    private SampleKBContext(SampleKBLoader loader){
        this.loader = loader;
    }

    public static SampleKBContext empty(){
        return getContext(null);
    }

    public static SampleKBContext load(Consumer<GraknTx> build){
        return getContext(build);
    }

    public static SampleKBContext load(String ... files){
        return getContext((graknGraph) -> {
            for (String file : files) {
                SampleKBLoader.loadFromFile(graknGraph, file);
            }
        });
    }

    private static SampleKBContext getContext(@Nullable Consumer<GraknTx> preLoad){
        return new SampleKBContext(SampleKBLoader.preLoad(preLoad));
    }

    @Override
    protected List<TestRule> testRules() {
        return ImmutableList.of(SessionContext.create());
    }

    public EmbeddedGraknTx<?> tx() {
        checkInContext();
        return loader.tx();
    }

    public void rollback() {
        checkInContext();
        loader.rollback();
    }

    private void checkInContext() {
        Preconditions.checkState(SessionContext.canUseTx(), "EmbeddedCassandraContext may not have started");
    }
}
