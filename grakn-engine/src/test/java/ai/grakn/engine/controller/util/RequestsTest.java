package ai.grakn.engine.controller.util;

import ai.grakn.exception.GraknServerException;
import mjson.Json;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import static ai.grakn.engine.controller.util.Requests.extractJsonField;

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
        String nestedFieldValue = "nestedFieldValue";
        Json input = Json.object("topLevelField",
            Json.object("incorrectlyNamedNestedField", nestedFieldValue)
        );

        // test if exception is properly thrown
        boolean errorMessageThrown_ContainingMissingFieldInfo;
        try {
            extractJsonField(input, "topLevelField", "nestedField").asString();
            errorMessageThrown_ContainingMissingFieldInfo = false;
        } catch (GraknServerException e) {
            errorMessageThrown_ContainingMissingFieldInfo = e.getMessage().contains("nestedField");
        }
        assertThat(errorMessageThrown_ContainingMissingFieldInfo, equalTo(true));
    }
}
