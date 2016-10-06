package io.mindmaps;

import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.ConcurrencyTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class CommonTests extends MindmapsOrientDBTestBase {

    @Test
    public void ConcurrencyTest1() throws ExecutionException, InterruptedException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataSequentially(graph);
    }

    @Ignore //TODO: Fix this test
    @Test
    public void ConcurrencyTest2() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailure(graph);
    }

    @Ignore //TODO: Fix this test
    @Test
    public void ConcurrencyTest3() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite(graph);
    }
}
