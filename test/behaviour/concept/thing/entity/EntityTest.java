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

package com.vaticle.typedb.core.test.behaviour.concept.thing.entity;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        strict = true,
        plugin = "pretty",
        glue = "com.vaticle.typedb.core.test.behaviour",
        features = "external/vaticle_typedb_behaviour/concept/thing/entity.feature",
        tags = "not @ignore and not @ignore-typedb"
)
public class EntityTest {
    // ATTENTION:
    // When you click RUN from within this class through Intellij IDE, it will fail.
    // You can fix it by doing:
    //
    // 1) Go to 'Run'
    // 2) Select 'Edit Configurations...'
    // 3) Select 'Bazel test DatabaseTest'
    //
    // 4) Ensure 'Target Expression' is set correctly: '//<this>/<package>/<name>:test'
    //
    // 5) Update 'Bazel Flags':
    //    a) Remove the line that says: '--test_filter=com.vaticle.typedb.core.*'
    //    b) Use the following Bazel flags:
    //       --cache_test_results=no : to make sure you're not using cache
    //       --test_output=streamed : to make sure all output is printed
    //       --subcommands : to print the low-level commands and execution paths
    //       --sandbox_debug : to keep the sandbox not deleted after test runs
    //       --spawn_strategy=standalone : if you're on Mac and the tests need permission to access filesystem
    //
    // 6) Hit the RUN button by selecting the test from the dropdown menu on the top bar
}
