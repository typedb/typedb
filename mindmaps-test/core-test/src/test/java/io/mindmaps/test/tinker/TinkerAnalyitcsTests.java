package io.mindmaps.test.tinker;

import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.internal.analytics.Analytics;
import io.mindmaps.test.graql.analytics.AnalyticsTest;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class TinkerAnalyitcsTests extends MindmapsTinkerTestBase {

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testAkoIsAccountedForInSubgrap() throws Exception {
        AnalyticsTest.testAkoIsAccountedForInSubgraph(graph);
    }

    @Test
    public void AnalyticsTest_testCount() throws Exception {
        AnalyticsTest.testCount(graph, factory);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegrees() throws Exception {
        AnalyticsTest.testDegrees(graph, factory);
    }

    @Ignore //Ignored due to being expensive
    @Test
    public void AnalyticsTest_testDegreesAndPersist() throws Exception {
        AnalyticsTest.testDegreesAndPersist(graph, factory);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrect() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrect(graph);
    }

    @Ignore //Ignored due to being expensive
    @Test
    public void AnalyticsTest_testDegreeIsPersisted() throws Exception {
        AnalyticsTest.testDegreeIsPersisted(graph, factory);
    }

    @Ignore //Ignored due to being expensive
    @Test
    public void AnalyticsTest_testDegreeIsPersistedInPresenceOfOtherResource() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsPersistedInPresenceOfOtherResource(graph, factory);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrectAssertionAboutAssertion() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrectAssertionAboutAssertion(graph);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrectTernaryRelationships() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrectTernaryRelationships(graph, factory);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrectOneRoleplayerMultipleRole() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrectOneRoleplayerMultipleRoles(graph);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrectMissingRoleplayer() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrectMissingRoleplayer(graph, factory);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testDegreeIsCorrectRoleplayerWrongTyp() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testDegreeIsCorrectRoleplayerWrongType(graph);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testMultipleExecutionOfDegreeAndPersistWhileAddingNodes() throws InterruptedException, ExecutionException, MindmapsValidationException {
        AnalyticsTest.testMultipleExecutionOfDegreeAndPersistWhileAddingNodes(graph);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void AnalyticsTest_testComputingUsingDegreeResource() throws MindmapsValidationException {
        AnalyticsTest.testComputingUsingDegreeResource(graph);
    }

    @Ignore //TODO: Fix on TinkerGraphComputer
    @Test
    public void testNullResourceDoesntBreakAnalytics() throws MindmapsValidationException {
        AnalyticsTest.testNullResourceDoesntBreakAnalytics(graph);
    }
}
