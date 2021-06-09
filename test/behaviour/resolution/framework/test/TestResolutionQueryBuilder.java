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
import com.vaticle.typeql.lang.pattern.variable.Variable;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestResolutionQueryBuilder {

    @Test
    public void testIdVariablesAreRemovedCorrectly() {
        Set<Variable> variablesWithIds = TypeQL.parsePattern(
                "$transaction has currency $currency;\n" +
                "$transaction id V86232;\n" +
                "$currency id V36912;\n" +
                "$transaction isa transaction;\n"
        ).asConjunction().variables().collect(Collectors.toSet());

        Set<Variable> expectedVariables = TypeQL.parsePattern(
                "$transaction has currency $currency;\n" +
                "$transaction isa transaction;\n"
        ).asConjunction().variables().collect(Collectors.toSet());
        expectedVariables.add(null);

        Set<Variable> variablesWithoutIds = variablesWithIds.stream()
                .map(ResolutionQueryBuilder::removeIdProperties).collect(Collectors.toSet());

        assertEquals(expectedVariables, variablesWithoutIds);
    }
}
