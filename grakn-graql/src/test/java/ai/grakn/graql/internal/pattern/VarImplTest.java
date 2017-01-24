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

package ai.grakn.graql.internal.pattern;

import ai.grakn.graql.VarName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static ai.grakn.util.ErrorMessage.UNSUPPORTED_RESOURCE_VALUE;

public class VarImplTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void whenSettingTheValueToAValidDataTypeNoErrorIsThrown() {
        new VarImpl().value(3L);
        new VarImpl().value(3d);
        new VarImpl().value(false);
        new VarImpl().value("hi");
    }

    @Test
    public void whenSettingTheValueToAnInvalidDataTypeAnErrorIsThrown() {
        VarImpl var = new VarImpl();
        VarName value = VarName.of("hello");

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(UNSUPPORTED_RESOURCE_VALUE.getMessage(value, VarName.class.getName()));
        var.value(value);
    }
}