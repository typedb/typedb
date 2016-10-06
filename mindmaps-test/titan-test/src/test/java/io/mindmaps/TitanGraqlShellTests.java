package io.mindmaps;

import io.mindmaps.graph.graql.shell.GraqlShellIT;
import org.junit.Test;

import java.io.IOException;

public class TitanGraqlShellTests extends MindmapsTitanTestBase{
    //Graql Shell Tests
    @Test
    public void GraqlShellIT_testStartAndExitShell() throws IOException {
        GraqlShellIT.testStartAndExitShell(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testHelpOption() throws IOException {
        GraqlShellIT.testHelpOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testVersionOption() throws IOException {
        GraqlShellIT.testVersionOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testExecuteOption() throws IOException {
        GraqlShellIT.testExecuteOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testFileOption() throws IOException {
        GraqlShellIT.testFileOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testMatchQuer() throws IOException {
        GraqlShellIT.testMatchQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testAskQuery() throws IOException {
        GraqlShellIT.testAskQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testInsertQuery() throws IOException {
        GraqlShellIT.testInsertQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testInsertOutput() throws IOException {
        GraqlShellIT.testInsertOutput(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testAutocomplete() throws IOException {
        GraqlShellIT.testAutocomplete(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testAutocompleteShellCommand() throws IOException {
        GraqlShellIT.testAutocompleteShellCommand(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testAutocompleteFill() throws IOException {
        GraqlShellIT.testAutocompleteFill(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testReasoner() throws IOException {
        GraqlShellIT.testReasoner(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testInvalidQuery() throws IOException {
        GraqlShellIT.testInvalidQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testComputeCount() throws IOException {
        GraqlShellIT.testComputeCount(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testRollback() throws IOException {
        GraqlShellIT.testRollback(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testErrorWhenEngineNotRunning() throws IOException {
        GraqlShellIT.testErrorWhenEngineNotRunning(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_fuzzTest() throws IOException {
        GraqlShellIT.fuzzTest(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_testLargeQuery() throws IOException {
        GraqlShellIT.testLargeQuery(graph.getKeyspace());
    }
}
