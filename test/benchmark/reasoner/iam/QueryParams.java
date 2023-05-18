package com.vaticle.typedb.core.reasoner.benchmark.iam;

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.FileNotFoundException;
import java.nio.file.Path;

public class QueryParams {
    private static final Path paramsFile = BenchmarkRunner.RESOURCE_DIRECTORY.resolve("query_params.yml");

    final String permissionEmail;
    final String permissionObject;
    final String permissionAction;
    final String segregationEmail;
    final String segregationObject;
    final String segregationAction1;
    final String segregationAction2;
    final String largeNegationEmail;
    final String basicTestObject;

    public QueryParams(String permissionEmail, String permissionObject, String permissionAction,
                       String segregationEmail, String segregationObject, String segregationAction1, String segregationAction2,
                       String largeNegationEmail, String basicTestObject) {
        this.permissionEmail = permissionEmail;
        this.permissionObject = permissionObject;
        this.permissionAction = permissionAction;
        this.segregationEmail = segregationEmail;
        this.segregationObject = segregationObject;
        this.segregationAction1 = segregationAction1;
        this.segregationAction2 = segregationAction2;
        this.largeNegationEmail = largeNegationEmail;
        this.basicTestObject = basicTestObject;
    }

    private static String get(YAML.Map yamlMap, String key) {
        return yamlMap.get(key).asString().value();
    }

    static QueryParams load() {
        YAML.Map yamlMap = null;
        try {
            yamlMap = YAML.load(paramsFile).asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(e);
        }
        return new QueryParams(
                get(yamlMap, "permission_email"), get(yamlMap, "permission_object"), get(yamlMap, "permission_action"),
                get(yamlMap, "segregation_email"), get(yamlMap, "segregation_object"), get(yamlMap, "segregation_action1"), get(yamlMap, "segregation_action2"),
                get(yamlMap, "largeNegation_email"), get(yamlMap, "basicTest_object"));
    }
}
