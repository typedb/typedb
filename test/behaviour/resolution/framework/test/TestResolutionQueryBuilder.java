/*
 * Copyright (C) 2021 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.resolution.framework.test;

import com.vaticle.typedb.core.test.behaviour.resolution.framework.resolve.ResolutionQueryBuilder;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.statement.Statement;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.Utils.getStatements;
import static org.junit.Assert.assertEquals;

public class TestResolutionQueryBuilder {

    @Test
    public void testIdStatementsAreRemovedCorrectly() {
        Set<Statement> statementsWithIds = getStatements(TypeQL.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction id V86232;\n" +
                "$currency id V36912;\n" +
                "$transaction isa transaction;\n"
        ));

        Set<Statement> expectedStatements = getStatements(TypeQL.parsePatternList(
                "$transaction has currency $currency;\n" +
                "$transaction isa transaction;\n"
        ));
        expectedStatements.add(null);

        Set<Statement> statementsWithoutIds = statementsWithIds.stream().map(ResolutionQueryBuilder::removeIdProperties).collect(Collectors.toSet());

        assertEquals(expectedStatements, statementsWithoutIds);
    }
}
