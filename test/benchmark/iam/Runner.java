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
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.WriterConfig;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Runner {

    static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Runner.class);
    private final PrintStream outputStream;

    private Runner() {
        this.outputStream = System.out;
    }

    public void runTestSuite(Benchmark.ReasonerBenchmarkSuite testClass) {
        Method[] testMethods = Arrays.stream(testClass.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class) && !m.isAnnotationPresent(Ignore.class))
                .toArray(size -> new Method[size]);

        for (Method testMethod : testMethods) {
            try {
                LOG.info("Running test method {}::{}", testClass.getClass().getSimpleName(), testMethod.getName());
                testClass.setUp();
                testMethod.invoke(testClass);
            } catch (AssertionError e) {
                // Nothing, we good.
            } catch (Exception e) {
                testClass.exception(testMethod.getName(), e);
                e.printStackTrace();
            } finally {
                testClass.tearDown();
            }
        }
    }

    public static void main(String[] args) {
        Logger root = (Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        Benchmark.ReasonerBenchmarkSuite[] testClasses = new Benchmark.ReasonerBenchmarkSuite[]{
                new ComplexConjunctionsTest(true),
                new LargeDataTest(true),
                new ComplexRuleGraphTest(true)
        };

        String runId = (args.length >= 1) ? args[0] : "reasoner benchmark";
        JsonObject fullSummary = Json.object();
        fullSummary.add("run_id", runId);

        Runner runner = new Runner();

        for (Benchmark.ReasonerBenchmarkSuite testClass : testClasses) {
            LOG.info("Running test class {}", testClass.getClass().getSimpleName());
            runner.runTestSuite(testClass);
            JsonObject testClassSummary = testClass.jsonSummary();
            fullSummary.add(testClass.getClass().getSimpleName(), testClassSummary);
        }
        LOG.info("Finished running all test classes");

        runner.outputStream.println("/* Printing full result */");
        runner.outputStream.println(fullSummary.toString(WriterConfig.PRETTY_PRINT));

        // You know we should be done here. I'm probably forgetting to close something
        System.exit(0);
    }
}
