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

package ai.grakn.test.benchmark;

import ai.grakn.GraknSystemProperty;
import io.netty.util.internal.SystemPropertyUtil;
import org.junit.Test;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static ai.grakn.test.benchmark.BenchmarkTest.DEFAULT_FORK;
import static ai.grakn.test.benchmark.BenchmarkTest.DEFAULT_MEASURE_ITERATIONS;
import static ai.grakn.test.benchmark.BenchmarkTest.DEFAULT_WARMUP_ITERATIONS;


/**
 * Base class for all JMH benchmarks.
 */
@Warmup(iterations = DEFAULT_WARMUP_ITERATIONS)
@Measurement(iterations = DEFAULT_MEASURE_ITERATIONS)
@Fork(DEFAULT_FORK)
@State(Scope.Thread)
public abstract class BenchmarkTest {

    static final int DEFAULT_FORK = 2;
    static final int DEFAULT_WARMUP_ITERATIONS = 1;
    static final int DEFAULT_MEASURE_ITERATIONS = 10;

    private ChainedOptionsBuilder newOptionsBuilder() throws Exception {
        String className = getClass().getSimpleName();

        ChainedOptionsBuilder runnerOptions = new OptionsBuilder()
                .include(".*" + className + ".*").detectJvmArgs();

        // We have to pass system properties into the child JVM
        // TODO: This should probably not be necessary
        List<String> jvmArgs = new ArrayList<>();

        for (GraknSystemProperty property : GraknSystemProperty.values()) {
            String value = property.value();
            if (value != null) {
                jvmArgs.add("-D" + property.key() + "=" + value);
            }
        }

        runnerOptions.jvmArgsAppend(jvmArgs.toArray(new String[jvmArgs.size()]));

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (getReportDir() != null) {
            String filePath = getReportDir() + className + ".json";
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        return runnerOptions;
    }

    @Test
    public void run() throws Exception {
        new Runner(newOptionsBuilder().build()).run();
    }

    private int getWarmupIterations() {
        return SystemPropertyUtil.getInt("warmupIterations", -1);
    }

    private int getMeasureIterations() {
        return SystemPropertyUtil.getInt("measureIterations", -1);
    }

    private String getReportDir() {
        return SystemPropertyUtil.get("perfReportDir", "./benchmarks/");
    }

}