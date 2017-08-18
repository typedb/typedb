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
import ai.grakn.test.SampleKBContext;

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
    protected void buildSchema(GraknTx graph) {
        key = graph.putAttributeType("name", AttributeType.DataType.STRING);

        TOEFL = graph.putAttributeType("TOEFL", AttributeType.DataType.LONG);
        GRE = graph.putAttributeType("GRE", AttributeType.DataType.LONG);
        vGRE = graph.putAttributeType("vGRE", AttributeType.DataType.LONG);
        GPR = graph.putAttributeType("GPR", AttributeType.DataType.DOUBLE);
        specialHonours = graph.putAttributeType("specialHonours", AttributeType.DataType.STRING);
        considerGPR = graph.putAttributeType("considerGPR", AttributeType.DataType.STRING);
        transcript = graph.putAttributeType("transcript", AttributeType.DataType.STRING);
        priorGraduateWork = graph.putAttributeType("priorGraduateWork", AttributeType.DataType.STRING);
        languageRequirement= graph.putAttributeType("languageRequirement", AttributeType.DataType.STRING);
        degreeOrigin = graph.putAttributeType("degreeOrigin", AttributeType.DataType.STRING);
        admissionStatus = graph.putAttributeType("admissionStatus", AttributeType.DataType.STRING);
        decisionType = graph.putAttributeType("decisionType", AttributeType.DataType.STRING);

        applicant = graph.putEntityType("applicant");
        applicant.attribute(TOEFL);
        applicant.attribute(GRE);
        applicant.attribute(vGRE);
        applicant.attribute(GPR);
        applicant.attribute(specialHonours);
        applicant.attribute(considerGPR);
        applicant.attribute(transcript);
        applicant.attribute(priorGraduateWork);
        applicant.attribute(languageRequirement);
        applicant.attribute(degreeOrigin);
        applicant.attribute(admissionStatus);
        applicant.attribute(decisionType);
        applicant.attribute(key);
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
        SampleKBContext.loadFromFile(graph, "admission-rules.gql");
    }
}
