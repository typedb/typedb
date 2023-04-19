package com.vaticle.typedb.core.reasoner.benchmark.iam;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class Runner {

    public void runTestSuite(Benchmark.ReasonerBenchmarkSuite testClass) {
        Method[] tests = Arrays.stream(testClass.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class) && !m.isAnnotationPresent(Ignore.class))
                .toArray(size -> new Method[size]);

        for (Method test : tests) {
            try {
                testClass.setUp();
                test.invoke(testClass);
            } catch (AssertionError e) {
                // Nothing, we good.
            } catch (Exception e) {
                testClass.exception(e);
                e.printStackTrace();
            } finally {
                testClass.tearDown();
            }
        }
    }

    public static void main(String[] args) {
        Benchmark.ReasonerBenchmarkSuite[] testClasses = new Benchmark.ReasonerBenchmarkSuite[]{
                new ComplexConjunctionsTest(true),
                new LargeDataTest(true),
                new ComplexRuleGraphTest(true)
        };
        Runner runner = new Runner();
        for (Benchmark.ReasonerBenchmarkSuite testClass : testClasses) {
            runner.runTestSuite(testClass);
            System.out.println(testClass.jsonSummary());
        }

        // You know we should be done here. I'm probably forgetting to close something
        System.exit(0);
    }
}
