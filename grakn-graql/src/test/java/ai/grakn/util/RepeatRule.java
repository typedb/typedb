/*
<<<<<<< HEAD
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
=======
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
>>>>>>> 88134929f3f653aa13bfa69ca7d0727344cc1e9a
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
<<<<<<< HEAD
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
=======
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
>>>>>>> 88134929f3f653aa13bfa69ca7d0727344cc1e9a
 */

package ai.grakn.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Repeat rule for {@link Repeat} annotation.
 *
 */
public class RepeatRule implements TestRule {

    private static class RepeatStatement extends Statement {

        private final int times;
        private final Statement statement;

        private RepeatStatement(int times, Statement statement) {
            this.times = times;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            for( int i = 0; i < times; i++ ) {
                statement.evaluate();
            }
        }
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        Statement result = statement;
        Repeat repeat = description.getAnnotation(Repeat.class);
        if( repeat != null ) {
            int times = repeat.times();
            result = new RepeatStatement(times, statement);
        }
        return result;
    }
}
