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
 *
 */

package ai.grakn.test;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.List;

/**
 * A {@link TestRule} that is composed of other {@link TestRule}s.
 *
 * @author Felix Chapman
 */
public abstract class CompositeResource implements TestRule {

    protected abstract List<TestRule> testRules();

    private ExternalResource innerResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            CompositeResource.this.before();
        }

        @Override
        protected void after() {
            CompositeResource.this.after();
        }
    };

    @Override
    public final Statement apply(Statement base, Description description) {
        base = innerResource.apply(base, description);

        for (TestRule each : testRules()) {
            base = each.apply(base, description);
        }

        return base;
    }

    protected void before() throws Throwable {

    }

    protected void after() {

    }
}
