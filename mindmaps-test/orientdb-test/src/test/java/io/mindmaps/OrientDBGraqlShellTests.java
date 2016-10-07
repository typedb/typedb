package io.mindmaps;

import io.mindmaps.shell.graql.GraqlShellIT;
import org.junit.Ignore;
import org.junit.Test;

public class OrientDBGraqlShellTests extends MindmapsOrientDBTestBase{

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testStartAndExitShell() throws Exception {
        GraqlShellIT.testStartAndExitShell(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testHelpOption() throws Exception {
        GraqlShellIT.testHelpOption(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test. When this runs a bunch of tests stall for some reason.
    @Test
    public void GraqlShellIT_testVersionOption() throws Exception {
        GraqlShellIT.testVersionOption(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testMatchQuery() throws Exception {
        GraqlShellIT.testMatchQuery(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testExecuteOption() throws Exception {
        GraqlShellIT.testExecuteOption(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testFileOption() throws Exception {
        GraqlShellIT.testFileOption(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testAskQuery() throws Exception {
        GraqlShellIT.testAskQuery(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testInsertQuery() throws Exception {
        GraqlShellIT.testInsertQuery(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testInsertOutput() throws Exception {
        GraqlShellIT.testInsertOutput(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testAutocomplete() throws Exception {
        GraqlShellIT.testAutocomplete(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testAutocompleteShellCommand() throws Exception {
        GraqlShellIT.testAutocompleteShellCommand(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testAutocompleteFill() throws Exception {
        GraqlShellIT.testAutocompleteFill(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testReasoner() throws Exception {
        GraqlShellIT.testReasoner(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testInvalidQuery() throws Exception {
        GraqlShellIT.testInvalidQuery(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testComputeCount() throws Exception {
        GraqlShellIT.testComputeCount(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testRollback() throws Exception {
        GraqlShellIT.testRollback(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testErrorWhenEngineNotRunning() throws Exception {
        GraqlShellIT.testErrorWhenEngineNotRunning(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_fuzzTest() throws Exception {
        GraqlShellIT.fuzzTest(graph.getKeyspace());
    }

    @Ignore //TODO: Fix this test.
    @Test
    public void GraqlShellIT_testLargeQuery() throws Exception {
        GraqlShellIT.testLargeQuery(graph.getKeyspace());
    }
}
