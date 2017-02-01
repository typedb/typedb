/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Ltd
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.test.engine.controller;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Resource;
import ai.grakn.example.MovieGraphFactory;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.InsertQuery;
import ai.grakn.test.EngineContext;
import ai.grakn.util.REST;
import com.jayway.restassured.response.Response;
import javafx.util.Pair;
import mjson.Json;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.PATH_FIELD;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.post;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class ImportControllerTest {

    @ClassRule
    public static final EngineContext engine = EngineContext.startDistributedServer();

    @Test
    public void loadInserts() throws Exception {
        List<String> movieNames = IntStream.range(0, 30)
                .mapToObj(t -> UUID.randomUUID().toString())
                .collect(toList());

        List<InsertQuery> inserts =  movieNames.stream()
                .map(this::insertQuery)
                .collect(toList());

        File file = createTemporaryFile(inserts);

        runAndAssertCorrect(file, movieNames, "grakn");
    }

    @Test
    public void loadInsertsInDifferentKeyspace() throws Exception {
        List<String> movieNames = IntStream.range(0, 10)
                .mapToObj(t -> UUID.randomUUID().toString())
                .collect(toList());

        List<InsertQuery> inserts =  movieNames.stream()
                .map(this::insertQuery)
                .collect(toList());

        File file = createTemporaryFile(inserts);

        runAndAssertCorrect(file, movieNames, "other");
    }

    @Test
    public void loadMatchInserts() throws Exception {
        List<Pair<String, String>> movieAndPeopleNames = IntStream.range(0, 30)
                .mapToObj(t -> new Pair<>(UUID.randomUUID().toString(), UUID.randomUUID().toString()))
                .collect(toList());

        List<InsertQuery> inserts =  movieAndPeopleNames.stream()
                .map(Pair::getKey)
                .map(this::insertQuery)
                .collect(toList());

        List<InsertQuery> matchInserts = movieAndPeopleNames.stream()
                .map(p -> matchInsertQuery(p.getKey(), p.getValue()))
                .collect(toList());

        File insertFile = createTemporaryFile(inserts);
        runAndAssertCorrect(insertFile, movieAndPeopleNames.stream().map(Pair::getKey).collect(toList()), "grakn");

        File matchInsertFile = createTemporaryFile(matchInserts);
        runAndAssertCorrect(matchInsertFile, movieAndPeopleNames.stream().map(Pair::getValue).collect(toList()), "grakn");
    }

    private void runAndAssertCorrect(File file, List<String> findByResource, String keyspace)
            throws GraknValidationException {
        GraknGraph graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
        MovieGraphFactory.loadGraph(graph);
        graph.commit();
        
        post(REST.WebPath.IMPORT_DATA_URI).then().assertThat().statusCode(200);

        Response dataResponse = given()
                .contentType("application/json")
                .queryParam(KEYSPACE_PARAM, keyspace)
                .body(Json.object(PATH_FIELD, file.getAbsolutePath()).toString())
                .when().post(REST.WebPath.IMPORT_DATA_URI);


        waitToFinish();

        findByResource.forEach(r -> {
            Collection<Resource<String>> resources = graph.getResourcesByValue(r);
            resources.forEach(Assert::assertNotNull);
            resources.stream().map(Resource::owner).forEach(Assert::assertNotNull);
        });

        graph.clear();
        graph.close();
    }

    private void waitToFinish() {
        while (true) {
            Response response = post(REST.WebPath.IMPORT_DATA_URI);
            if (response.statusCode() != 423)
                break;

            try {
                Thread.sleep(100);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private InsertQuery insertQuery(String name){
        return insert(
                var("movie").isa("movie").has("title", var("name")),
                var("name").value(name));
    }

    private InsertQuery matchInsertQuery(String movieName, String personName) {

        return match(var("movie").isa("movie").has("title", movieName))
                .insert(
                        var("person").isa("person").has("name", personName),
                        var().rel(var("movie")).rel(var("person")).isa("has-cast")
                );
    }

    private File createTemporaryFile(List<InsertQuery> queries){
        try {
            File temp = File.createTempFile("queries" + UUID.randomUUID().toString(), ".gql");

            BufferedWriter writer = new BufferedWriter(new FileWriter(temp));
            writer.write(queries.stream().map(InsertQuery::toString).collect(joining("\n")));
            writer.close();

            return temp;
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}