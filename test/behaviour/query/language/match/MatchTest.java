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

package com.vaticle.typedb.core.test.behaviour.query.language.match;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        strict = true,
        plugin = "pretty",
        glue = "com.vaticle.typedb.core.test.behaviour",
        features = "external/vaticle_typedb_behaviour/query/language/match.feature",
        tags = "not @ignore and not @ignore-typedb"
)
public class MatchTest {
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
