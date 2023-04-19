package com.vaticle.typedb.core.reasoner.benchmark.iam;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

public class Runner {

    public static void runTestSuite(Benchmark.ReasonerBenchmarkSuite testClass) {
        Method[] tests = Arrays.stream(testClass.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(Test.class) && !m.isAnnotationPresent(Ignore.class))
                .toArray(size -> new Method[size]);

        for (Method test : tests) {
            try {
                testClass.setUp();
                test.invoke(testClass);
                testClass.tearDown();
            } catch (Exception e) {
                // TODO
            }
        }
    }

    public static void main(String[] args) {
        Benchmark.ReasonerBenchmarkSuite[] testClasses = new Benchmark.ReasonerBenchmarkSuite[]{
                new ComplexConjunctionsTest(),
                new LargeDataTest(),
                new ComplexRuleGraphTest()
        };
        for (Benchmark.ReasonerBenchmarkSuite testClass : testClasses) {
            runTestSuite(testClass);
        }
    }
}
