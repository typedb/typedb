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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.template.macro;

import ai.grakn.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

import static ai.grakn.graql.internal.template.macro.MacroTestUtilities.assertParseEquals;
import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;

public class SplitMacroTest {

    private final SplitMacro splitMacro = new SplitMacro();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void applySplitMacroToNoArguments_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        splitMacro.apply(Collections.emptyList());
    }

    @Test
    public void applySplitMacroThanOneArgument_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        splitMacro.apply(ImmutableList.of("a"));
    }

    @Test
    public void applySplitMacroToMoreThanTwo_ExceptionIsThrown(){
        exception.expect(GraqlQueryException.class);

        splitMacro.apply(ImmutableList.of("a", "b", "c"));
    }

    @Test
    public void applySplitMacroToTwoArguments_ResultIsNonNull(){
        assertNotNull(splitMacro.apply(ImmutableList.of("a,b", ",")));
    }

    @Test
    public void applySplitMacroToTwoOrMoreArguments_ResultIsNonNull(){
        String toSplit = "a,b";
        String splitCharacter = ",";

        List<String> afterSplit = ImmutableList.of("a", "b");
        List<String> split = splitMacro.apply(ImmutableList.of(toSplit, splitCharacter));

        assertEquals(afterSplit, split);
    }

    @Test
    public void whenUsingSplitMacroInTemplate_ItExecutesCorrectly() {
        String template = "insert $x for (val in @split(<list>, \",\") ) do { has description <val>};";
        String expected = "insert $x0 has description \"orange\" has description \"cat\" has description \"dog\";";

        assertParseEquals(template, Collections.singletonMap("list", "cat,dog,orange"), expected);
    }
}
