/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package ai.grakn.exception;

import ai.grakn.concept.AttributeType.DataType;
import org.junit.Test;

import java.time.LocalDateTime;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class GraknTxOperationExceptionTest {

    @Test
    public void whenGettingErrorMessageForInvalidAttributeValue_MessageIncludesExpectedClass() {
        String message = GraknTxOperationException.invalidAttributeValue("bob", DataType.DATE).getMessage();
        assertThat(message, containsString(LocalDateTime.class.getName()));
    }

}