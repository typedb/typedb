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

package ai.grakn.test.rule;

import ai.grakn.GraknTx;
import ai.grakn.util.GraknTestUtil;
import com.google.common.collect.ImmutableList;
import org.junit.rules.TestRule;

import java.util.List;

/**
 * Context for tests that use {@link GraknTx}s. Will make sure that any dependencies such as cassandra are running
 *
 * @author Felix Chapman
 */
public class TxFactoryContext extends CompositeTestRule {

    private TxFactoryContext() {
    }

    public static TxFactoryContext create() {
        return new TxFactoryContext();
    }

    @Override
    protected List<TestRule> testRules() {
        if (GraknTestUtil.usingJanus()) {
            return ImmutableList.of(EmbeddedCassandraContext.create());
        } else {
            return ImmutableList.of();
        }
    }

    public static boolean canUseTx() {
        return !GraknTestUtil.usingJanus() || EmbeddedCassandraContext.inCassandraContext();
    }
}
