/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.reasoner.benchmark.iam.common;

import com.vaticle.typedb.common.yaml.YAML;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.io.FileNotFoundException;
import java.nio.file.Path;

public class QueryParams {
    private static final Path paramsFile = BenchmarkRunner.RESOURCE_DIRECTORY.resolve("query_params.yml");

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

    public static QueryParams load() {
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
