package grakn.core.test.behaviour.resolution.test;

import grakn.core.test.behaviour.resolution.Resolution;
import grakn.core.test.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.query.GraqlGet;
import org.junit.ClassRule;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestResolution {

    @ClassRule
    public static final GraknTestServer graknTestServer = new GraknTestServer();

    @Test
    public void testCase1HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case1", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $lh (location-hierarchy_superior: $continent, " +
                "location-hierarchy_subordinate: $area) isa location-hierarchy; " +
                "$continent isa continent; " +
                "$area isa area; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase2HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case2", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case2", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $transaction has currency $currency; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase3HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case3", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case3", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match (sibling: $p, sibling: $p1) isa siblingship; $p != $p1; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase4HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case4", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case4", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $com isa company, has is-liable $lia; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase5HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case5", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case5", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("match $c isa company, has name $n; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase6HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case6", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case6", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $com isa company; " +
                "{$com has name \"the-company\";} or {$com has name \"another-company\";}; " +
                "not {$com has is-liable $liability;}; " +
                "get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }

    @Test
    public void testCase7HappyPath() {
        Path schemaPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case7", "schema.gql").toAbsolutePath();
        Path dataPath = Paths.get("test", "behaviour", "resolution", "test", "cases", "case7", "data.gql").toAbsolutePath();
        GraqlGet inferenceQuery = Graql.parse("" +
                "match $com isa company, has is-liable $lia; $lia true; get;").asGet();

        Resolution resolution_test = new Resolution(graknTestServer, schemaPath, dataPath);
        resolution_test.testQuery(inferenceQuery);
        resolution_test.testCompleteness();
        resolution_test.close();
    }
}

