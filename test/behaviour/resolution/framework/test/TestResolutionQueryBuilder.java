/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.test.behaviour.resolution.framework.test;

import grakn.core.test.behaviour.resolution.framework.resolve.ResolutionQueryBuilder;
import graql.lang.Graql;
import graql.lang.statement.Statement;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static grakn.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static org.junit.Assert.assertEquals;

public class TestResolutionQueryBuilder {

    @Test
    public void testIdStatementsAreRemovedCorrectly() {
        Set<Statement> statementsWithIds = getStatements(Graql.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction id V86232;\n" +
                "$currency id V36912;\n" +
                "$transaction isa transaction;\n"
        ));

        Set<Statement> expectedStatements = getStatements(Graql.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction isa transaction;\n"
        ));
        expectedStatements.add(null);

        Set<Statement> statementsWithoutIds = statementsWithIds.stream().map(ResolutionQueryBuilder::removeIdProperties).collect(Collectors.toSet());

        assertEquals(expectedStatements, statementsWithoutIds);
    }
}
