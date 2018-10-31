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

package ai.grakn.core.server.controller.util;

import ai.grakn.exception.GraknServerException;
import mjson.Json;
import org.junit.Test;

import static ai.grakn.core.server.controller.util.Requests.extractJsonField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


public class RequestsTest {
    @Test
    public void extractJsonField_MustExtractTopLevelFieldCorrectly() {
        String topLevelFieldValue = "topLevelFieldValue";
        Json input = Json.object("topLevelField", topLevelFieldValue);
        String extractedTopLevelField = extractJsonField(input, "topLevelField").asString();
        assertThat(extractedTopLevelField, equalTo(topLevelFieldValue));
    }

    @Test
    public void extractJsonField_MustExtractNestedFieldCorrectly() {
        String nestedFieldValue = "nestedFieldValue";
        Json input = Json.object("topLevelField",
            Json.object("nestedField", nestedFieldValue)
        );
        String extractedNestedField = extractJsonField(input, "topLevelField", "nestedField").asString();
        assertThat(extractedNestedField, equalTo(nestedFieldValue));
    }

    @Test
    public void extractJsonField_MustThrowInformativeError() {
        // in this test, extractJsonField expects the existence of a field "topLevelField.nestedField"
        // however, the input incorrectly contains "topLevelField.incorrectlyNamedNestedField" instead

        String nestedFieldValue = "nestedFieldValue";
        Json input = Json.object("topLevelField",
            Json.object("incorrectlyNamedNestedField", nestedFieldValue)
        );

        // test if exception is properly thrown
        boolean errorMessageThrown_ContainingMissingFieldInfo;
        try {
            extractJsonField(input, "topLevelField", "nestedField");
            errorMessageThrown_ContainingMissingFieldInfo = false;
        } catch (GraknServerException e) {
            errorMessageThrown_ContainingMissingFieldInfo = e.getMessage().contains("nestedField");
        }

        assertThat(errorMessageThrown_ContainingMissingFieldInfo, equalTo(true));
    }
}
