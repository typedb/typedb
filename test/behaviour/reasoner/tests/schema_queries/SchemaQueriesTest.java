/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.reasoner.tests.schema_queries;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        strict = true,
        plugin = "pretty",
        glue = "com.vaticle.typedb.core.test.behaviour",
        features = "external/vaticle_typedb_behaviour/query/reasoner/schema-queries.feature",
        tags = "not @ignore and not @ignore-typedb"
)
public class SchemaQueriesTest {
    // ATTENTION:
    // When you click RUN from within this class through Intellij IDE, it will fail.
    // You can fix it by doing:
    //
    // 1) Go to 'Run'
    // 2) Select 'Edit Configurations...'
    // 3) Select 'Bazel test <Name>'
    //
    // 4) Update 'Bazel Flags':
    //    a) Remove the line that says: '--test_filter=...'
    //    b) Use the following Bazel flags:
    //       --cache_test_results=no : to make sure you're not using cache
    //       --test_output=streamed : to make sure all output is printed
    //       --subcommands : to print the low-level commands and execution paths
    //
    // 5) Hit the RUN button by selecting the test from the dropdown menu on the top bar
}