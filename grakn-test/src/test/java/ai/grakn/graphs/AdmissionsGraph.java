/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.ResourceType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

public class AdmissionsGraph extends TestGraph{

    private static ResourceType<String> key;

    private static EntityType applicant;

    private static ResourceType<Long> TOEFL;
    private static ResourceType<Double> GPR;
    private static ResourceType<Long> GRE;
    private static ResourceType<Long> vGRE;
    private static ResourceType<String> specialHonours;
    private static ResourceType<String> degreeOrigin;
    private static ResourceType<String> transcript;
    private static ResourceType<String> priorGraduateWork;
    private static ResourceType<String> languageRequirement;
    private static ResourceType<String> considerGPR;
    private static ResourceType<String> admissionStatus;
    private static ResourceType<String> decisionType;

    public static Consumer<GraknGraph> get() {
        return new AdmissionsGraph().build();
    }

    @Override
    protected void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        TOEFL = graph.putResourceType("TOEFL", ResourceType.DataType.LONG);
        GRE = graph.putResourceType("GRE", ResourceType.DataType.LONG);
        vGRE = graph.putResourceType("vGRE", ResourceType.DataType.LONG);
        GPR = graph.putResourceType("GPR", ResourceType.DataType.DOUBLE);
        specialHonours = graph.putResourceType("specialHonours", ResourceType.DataType.STRING);
        considerGPR = graph.putResourceType("considerGPR", ResourceType.DataType.STRING);
        transcript = graph.putResourceType("transcript", ResourceType.DataType.STRING);
        priorGraduateWork = graph.putResourceType("priorGraduateWork", ResourceType.DataType.STRING);
        languageRequirement= graph.putResourceType("languageRequirement", ResourceType.DataType.STRING);
        degreeOrigin = graph.putResourceType("degreeOrigin", ResourceType.DataType.STRING);
        admissionStatus = graph.putResourceType("admissionStatus", ResourceType.DataType.STRING);
        decisionType = graph.putResourceType("decisionType", ResourceType.DataType.STRING);

        applicant = graph.putEntityType("applicant");
        applicant.resource(TOEFL);
        applicant.resource(GRE);
        applicant.resource(vGRE);
        applicant.resource(GPR);
        applicant.resource(specialHonours);
        applicant.resource(considerGPR);
        applicant.resource(transcript);
        applicant.resource(priorGraduateWork);
        applicant.resource(languageRequirement);
        applicant.resource(degreeOrigin);
        applicant.resource(admissionStatus);
        applicant.resource(decisionType);
        applicant.resource(key);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        Instance Alice = putEntity(graph, "Alice", applicant, key.getLabel());
        Instance Bob = putEntity(graph, "Bob", applicant, key.getLabel());
        Instance Charlie = putEntity(graph, "Charlie", applicant, key.getLabel());
        Instance Denis = putEntity(graph, "Denis", applicant, key.getLabel());
        Instance Eva = putEntity(graph, "Eva", applicant, key.getLabel());
        Instance Frank = putEntity(graph, "Frank", applicant, key.getLabel());

        putResource(Alice, TOEFL, 470L);
        putResource(Alice, degreeOrigin, "nonUS");

        putResource(Bob, priorGraduateWork, "none");
        putResource(Bob, TOEFL, 520L);
        putResource(Bob, degreeOrigin, "US");
        putResource(Bob, transcript, "unavailable");
        putResource(Bob, specialHonours, "none");
        putResource(Bob, GRE, 1100L);

        putResource(Charlie, priorGraduateWork, "none");
        putResource(Charlie, TOEFL, 600L);
        putResource(Charlie, degreeOrigin, "US");
        putResource(Charlie, transcript, "available");
        putResource(Charlie, specialHonours, "none");
        putResource(Charlie, GRE, 1100L);
        putResource(Charlie, vGRE, 400L);
        putResource(Charlie, GPR, 2.99);

        putResource(Denis, priorGraduateWork, "none");
        putResource(Denis, degreeOrigin, "US");
        putResource(Denis, transcript, "available");
        putResource(Denis, specialHonours, "none");
        putResource(Denis, GRE, 900L);
        putResource(Denis, vGRE, 350L);
        putResource(Denis, GPR, 2.5);

        putResource(Eva, priorGraduateWork, "completed");
        putResource(Eva, specialHonours, "valedictorian");
        putResource(Eva, GPR, 3.0);

        putResource(Frank, TOEFL, 550L);
        putResource(Frank, degreeOrigin, "US");
        putResource(Frank, transcript, "unavailable");
        putResource(Frank, specialHonours, "none");
        putResource(Frank, GRE, 100L);
    }

    @Override
    protected void buildRelations(GraknGraph graph) {

    }

    @Override
    protected void buildRules(GraknGraph graph) {
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/graql/admission-rules.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            graph.graql().parse(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
