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

package ai.grakn.test.graphs;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Thing;
import ai.grakn.test.GraphContext;

import java.util.function.Consumer;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class AdmissionsGraph extends TestGraph {

    private static AttributeType<String> key;

    private static EntityType applicant;

    private static AttributeType<Long> TOEFL;
    private static AttributeType<Double> GPR;
    private static AttributeType<Long> GRE;
    private static AttributeType<Long> vGRE;
    private static AttributeType<String> specialHonours;
    private static AttributeType<String> degreeOrigin;
    private static AttributeType<String> transcript;
    private static AttributeType<String> priorGraduateWork;
    private static AttributeType<String> languageRequirement;
    private static AttributeType<String> considerGPR;
    private static AttributeType<String> admissionStatus;
    private static AttributeType<String> decisionType;

    public static Consumer<GraknTx> get() {
        return new AdmissionsGraph().build();
    }

    @Override
    protected void buildOntology(GraknTx graph) {
        key = graph.putResourceType("name", AttributeType.DataType.STRING);

        TOEFL = graph.putResourceType("TOEFL", AttributeType.DataType.LONG);
        GRE = graph.putResourceType("GRE", AttributeType.DataType.LONG);
        vGRE = graph.putResourceType("vGRE", AttributeType.DataType.LONG);
        GPR = graph.putResourceType("GPR", AttributeType.DataType.DOUBLE);
        specialHonours = graph.putResourceType("specialHonours", AttributeType.DataType.STRING);
        considerGPR = graph.putResourceType("considerGPR", AttributeType.DataType.STRING);
        transcript = graph.putResourceType("transcript", AttributeType.DataType.STRING);
        priorGraduateWork = graph.putResourceType("priorGraduateWork", AttributeType.DataType.STRING);
        languageRequirement= graph.putResourceType("languageRequirement", AttributeType.DataType.STRING);
        degreeOrigin = graph.putResourceType("degreeOrigin", AttributeType.DataType.STRING);
        admissionStatus = graph.putResourceType("admissionStatus", AttributeType.DataType.STRING);
        decisionType = graph.putResourceType("decisionType", AttributeType.DataType.STRING);

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
    protected void buildInstances(GraknTx graph) {
        Thing Alice = putEntity(graph, "Alice", applicant, key.getLabel());
        Thing Bob = putEntity(graph, "Bob", applicant, key.getLabel());
        Thing Charlie = putEntity(graph, "Charlie", applicant, key.getLabel());
        Thing Denis = putEntity(graph, "Denis", applicant, key.getLabel());
        Thing Eva = putEntity(graph, "Eva", applicant, key.getLabel());
        Thing Frank = putEntity(graph, "Frank", applicant, key.getLabel());

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
    protected void buildRelations(GraknTx graph) {

    }

    @Override
    protected void buildRules(GraknTx graph) {
        GraphContext.loadFromFile(graph, "admission-rules.gql");
    }
}
