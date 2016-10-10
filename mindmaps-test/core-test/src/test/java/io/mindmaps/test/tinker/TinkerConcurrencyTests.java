package io.mindmaps.test.tinker;

import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.test.graph.ConcurrencyTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TinkerConcurrencyTests extends MindmapsTinkerTestBase {
    @Test
    public void ConcurrencyTest_testWritingTheSameDataSequentially() throws ExecutionException, InterruptedException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataSequentially(graph);
    }

    @Ignore //TODO: Fix this test
    @Test
    public void ConcurrencyTest_testWritingTheSameDataConcurrentlyWithRetriesOnFailure() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailure(graph);
    }

    @Ignore //TODO: Fix this test
    @Test
    public void ConcurrencyTest_testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite(graph);
    }
}
