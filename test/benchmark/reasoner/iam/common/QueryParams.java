/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam.common;

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.FileNotFoundException;
import java.nio.file.Path;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.JAVA_ERROR;

public class QueryParams {
    public final String permissionEmail;
    public final String permissionObject;
    public final String permissionAction;
    public final String segregationEmail;
    public final String segregationObject;
    public final String segregationAction1;
    public final String segregationAction2;
    public final String largeNegationEmail;
    public final String basicTestObject;

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

    public static QueryParams load(Path paramFile) {
        YAML.Map yamlMap = null;
        try {
            yamlMap = YAML.load(paramFile).asMap();
        } catch (FileNotFoundException e) {
            throw TypeDBException.of(JAVA_ERROR, e);
        }
        return new QueryParams(
                get(yamlMap, "permission_email"), get(yamlMap, "permission_object"), get(yamlMap, "permission_action"),
                get(yamlMap, "segregation_email"), get(yamlMap, "segregation_object"), get(yamlMap, "segregation_action1"), get(yamlMap, "segregation_action2"),
                get(yamlMap, "largeNegation_email"), get(yamlMap, "basicTest_object"));
    }
}
