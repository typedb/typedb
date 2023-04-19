package com.vaticle.typedb.core.reasoner.benchmark.iam;

import com.eclipsesource.json.Json;
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

    private final PrintStream outputStream;
    private final PrintStream updateStream;

    private Runner() {
        this.outputStream = System.out;
        this.updateStream = System.out;
    }

    public void runTestSuite(Benchmark.ReasonerBenchmarkSuite testClass) {
        Method[] testMethods = Arrays.stream(testClass.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class) && !m.isAnnotationPresent(Ignore.class))
                .toArray(size -> new Method[size]);

        for (Method testMethod : testMethods) {
            try {
                updateStream.println("/* - - Running test method " + testClass.getClass().getSimpleName() + "::" + testMethod.getName() + " */");
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

        Runner runner = new Runner();
        runner.updateStream.println(String.format("/* %s */", (args.length >= 1) ? args[0] : " start runner "));

        for (Benchmark.ReasonerBenchmarkSuite testClass : testClasses) {
            runner.updateStream.println("/* Running test class " + testClass.getClass().getSimpleName() + " */");
            runner.runTestSuite(testClass);
            runner.outputStream.println(testClass.jsonSummary().toString(WriterConfig.PRETTY_PRINT));
        }
        runner.updateStream.println("/* End runner */");
        // You know we should be done here. I'm probably forgetting to close something
        System.exit(0);
    }
}
