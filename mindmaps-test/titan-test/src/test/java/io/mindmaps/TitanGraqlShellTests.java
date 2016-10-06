package io.mindmaps;

import io.mindmaps.graph.graql.shell.GraqlShellIT;
import org.junit.Test;

import java.io.IOException;

public class TitanGraqlShellTests extends MindmapsTitanTestBase{
    //Graql Shell Tests
    @Test
    public void GraqlShellIT_1() throws IOException {
        GraqlShellIT.testStartAndExitShell(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_2() throws IOException {
        GraqlShellIT.testHelpOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_3() throws IOException {
        GraqlShellIT.testVersionOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_4() throws IOException {
        GraqlShellIT.testExecuteOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_5() throws IOException {
        GraqlShellIT.testFileOption(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_6() throws IOException {
        GraqlShellIT.testMatchQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_7() throws IOException {
        GraqlShellIT.testAskQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_8() throws IOException {
        GraqlShellIT.testInsertQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_10() throws IOException {
        GraqlShellIT.testInsertOutput(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_11() throws IOException {
        GraqlShellIT.testAutocomplete(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_12() throws IOException {
        GraqlShellIT.testAutocompleteShellCommand(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_13() throws IOException {
        GraqlShellIT.testAutocompleteFill(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_14() throws IOException {
        GraqlShellIT.testReasoner(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_15() throws IOException {
        GraqlShellIT.testInvalidQuery(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_16() throws IOException {
        GraqlShellIT.testComputeCount(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_17() throws IOException {
        GraqlShellIT.testRollback(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_18() throws IOException {
        GraqlShellIT.testErrorWhenEngineNotRunning(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_19() throws IOException {
        GraqlShellIT.fuzzTest(graph.getKeyspace());
    }

    @Test
    public void GraqlShellIT_20() throws IOException {
        GraqlShellIT.testLargeQuery(graph.getKeyspace());
    }
}
