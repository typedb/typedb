package io.mindmaps;

import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graph.ConcurrencyTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TitanConcurrencyTests extends MindmapsTitanTestBase {

    //Concurrency Tests
    @Test
    public void ConcurrencyTest_testWritingTheSameDataSequentially() throws ExecutionException, InterruptedException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataSequentially(graph);
    }

    @Ignore //TODO: Fix this test
    @Test
    public void ConcurrencyTest_testWritingTheSameDataConcurrentlyWithRetriesOnFailure() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailure(graph);
    }

    @Test
    public void ConcurrencyTest_testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite() throws InterruptedException, ExecutionException, MindmapsValidationException {
        ConcurrencyTest.testWritingTheSameDataConcurrentlyWithRetriesOnFailureAndInitialDataWrite(graph);
    }
}
