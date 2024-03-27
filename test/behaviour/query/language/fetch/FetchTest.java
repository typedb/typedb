/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.query.language.fetch;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        strict = true,
        plugin = "pretty",
        glue = "com.vaticle.typedb.core.test.behaviour",
        features = "external/vaticle_typedb_behaviour/query/language/fetch.feature",
        tags = "not @ignore and not @ignore-typedb"
)
public class FetchTest {
    // ATTENTION:
    // When you click RUN from within this class through Intellij IDE, it will fail.
    // You can fix it by doing:
    //
    // 1) Go to 'Run'
    // 2) Select 'Edit Configurations...'
    // 3) Select 'Bazel test GetTest'
    //
    // 4) Ensure 'Target Expression' is set correctly:
    //    a) Use '//<this>/<package>/<name>:test-core' to test against typedb
    //    b) Use '//<this>/<package>/<name>:test-kgms' to test against typedb-cluster
    //
    // 5) Update 'Bazel Flags':
    //    a) Remove the line that says: '--test_filter=com.vaticle.typedb.core.*'
    //    b) Use the following Bazel flags:
    //       --cache_test_results=no : to make sure you're not using cache
    //       --test_output=streamed : to make sure all output is printed
    //       --subcommands : to print the low-level commands and execution paths
    //       --sandbox_debug : to keep the sandbox not deleted after test runs
    //       --spawn_strategy=standalone : if you're on Mac, tests need permission to access filesystem (to run TypeDB)
    //
    // 6) Hit the RUN button by selecting the test from the dropdown menu on the top bar
}
