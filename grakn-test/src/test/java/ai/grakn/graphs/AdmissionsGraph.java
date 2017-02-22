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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;

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
    private static RelationType TOEFLrelation;
    private static RoleType TOEFLvalue, TOEFLtarget;

    private static ResourceType<Double> GPR;
    private static RelationType GPRrelation;
    private static RoleType GPRvalue, GPRtarget;

    private static ResourceType<Long> GRE;
    private static RelationType GRErelation;
    private static RoleType GREvalue, GREtarget;

    private static ResourceType<Long> vGRE;
    private static RelationType vGRErelation;
    private static RoleType vGREvalue, vGREtarget;

    private static ResourceType<String> specialHonours;
    private static RelationType specialHonoursRelation;
    private static RoleType specialHonoursValue, specialHonoursTarget;

    private static ResourceType<String> degreeOrigin;
    private static RelationType degreeOriginRelation;
    private static RoleType degreeOriginValue, degreeOriginTarget;

    private static ResourceType<String> transcript;
    private static RelationType transcriptRelation;
    private static RoleType transcriptValue, transcriptTarget;

    private static ResourceType<String> priorGraduateWork;
    private static RelationType priorGraduateWorkRelation;
    private static RoleType priorGraduateWorkValue, priorGraduateWorkTarget;

    private static ResourceType<String> languageRequirement;
    private static RelationType languageRequirementRelation;
    private static RoleType languageRequirementValue, languageRequirementTarget;

    private static ResourceType<String> considerGPR;
    private static RelationType considerGPRrelation;
    private static RoleType considerGPRvalue, considerGPRtarget;

    private static RoleType admissionStatusTarget, admissionStatusValue;
    private static RelationType admissionStatusRelation;
    private static ResourceType<String> admissionStatus;

    private static RoleType decisionTypeTarget,  decisionTypeValue;
    private static RelationType decisionTypeRelation;
    private static ResourceType<String> decisionType;

    public static Consumer<GraknGraph> get() {
        return new AdmissionsGraph().build();
    }

    @Override
    protected void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        TOEFLtarget= graph.putRoleType("has-TOEFL-owner");
        TOEFLvalue = graph.putRoleType("has-TOEFL-value");
        TOEFLrelation = graph.putRelationType("has-TOEFL")
                .hasRole(TOEFLtarget).hasRole(TOEFLvalue);
        TOEFL = graph.putResourceType("TOEFL", ResourceType.DataType.LONG).playsRole(TOEFLvalue);

        GREtarget= graph.putRoleType("has-GRE-owner");
        GREvalue = graph.putRoleType("has-GRE-value");
        GRErelation = graph.putRelationType("has-GRE")
                .hasRole(GREtarget).hasRole(GREvalue);
        GRE = graph.putResourceType("GRE", ResourceType.DataType.LONG).playsRole(GREvalue);

        vGREtarget= graph.putRoleType("has-vGRE-owner");
        vGREvalue = graph.putRoleType("has-vGRE-value");
        vGRErelation = graph.putRelationType("has-vGRE")
                .hasRole(vGREtarget).hasRole(vGREvalue);
        vGRE = graph.putResourceType("vGRE", ResourceType.DataType.LONG).playsRole(vGREvalue);

        GPRtarget= graph.putRoleType("has-GPR-owner");
        GPRvalue = graph.putRoleType("has-GPR-value");
        GPRrelation = graph.putRelationType("has-GPR")
                .hasRole(GPRtarget).hasRole(GPRvalue);
        GPR = graph.putResourceType("GPR", ResourceType.DataType.DOUBLE).playsRole(GPRvalue);

        specialHonoursTarget= graph.putRoleType("has-specialHonours-owner");
        specialHonoursValue = graph.putRoleType("has-specialHonours-value");
        specialHonoursRelation = graph.putRelationType("has-specialHonours")
                .hasRole(specialHonoursTarget).hasRole(specialHonoursValue);
        specialHonours = graph.putResourceType("specialHonours", ResourceType.DataType.STRING)
                .playsRole(specialHonoursValue);

        considerGPRtarget= graph.putRoleType("has-considerGPR-owner");
        considerGPRvalue = graph.putRoleType("has-considerGPR-value");
        considerGPRrelation = graph.putRelationType("has-considerGPR")
                .hasRole(considerGPRtarget).hasRole(considerGPRvalue);
        considerGPR = graph.putResourceType("considerGPR", ResourceType.DataType.STRING).playsRole(considerGPRvalue);

        transcriptTarget = graph.putRoleType("has-transcript-owner");
        transcriptValue = graph.putRoleType("has-transcript-value");
        transcriptRelation = graph.putRelationType("has-transcript")
                .hasRole(transcriptTarget).hasRole(transcriptValue);
        transcript = graph.putResourceType("transcript", ResourceType.DataType.STRING)
                .playsRole(transcriptValue);

        priorGraduateWorkTarget = graph.putRoleType("has-priorGraduateWork-owner");
        priorGraduateWorkValue = graph.putRoleType("has-priorGraduateWork-value");
        priorGraduateWorkRelation = graph.putRelationType("has-priorGraduateWork")
                .hasRole(priorGraduateWorkTarget).hasRole(priorGraduateWorkValue);
        priorGraduateWork = graph.putResourceType("priorGraduateWork", ResourceType.DataType.STRING)
                .playsRole(priorGraduateWorkValue);

        languageRequirementTarget = graph.putRoleType("has-languageRequirement-owner");
        languageRequirementValue = graph.putRoleType("has-languageRequirement-value");
        languageRequirementRelation = graph.putRelationType("has-languageRequirement")
                .hasRole(languageRequirementTarget).hasRole(languageRequirementValue);
        languageRequirement= graph.putResourceType("languageRequirement", ResourceType.DataType.STRING)
                .playsRole(languageRequirementValue);

        degreeOriginTarget = graph.putRoleType("has-degreeOrigin-owner");
        degreeOriginValue = graph.putRoleType("has-degreeOrigin-value");
        degreeOriginRelation = graph.putRelationType("has-degreeOrigin")
                .hasRole(degreeOriginTarget).hasRole(degreeOriginValue);
        degreeOrigin = graph.putResourceType("degreeOrigin", ResourceType.DataType.STRING)
                .playsRole(degreeOriginValue);

        admissionStatusTarget = graph.putRoleType("has-admissionStatus-owner");
        admissionStatusValue = graph.putRoleType("has-admissionStatus-value");
        admissionStatusRelation = graph.putRelationType("has-admissionStatus")
                .hasRole(admissionStatusTarget).hasRole(admissionStatusValue);
        admissionStatus = graph.putResourceType("admissionStatus", ResourceType.DataType.STRING)
                .playsRole(admissionStatusValue);

        decisionTypeTarget = graph.putRoleType("has-decisionType-owner");
        decisionTypeValue = graph.putRoleType("has-decisionType-value");
        decisionTypeRelation = graph.putRelationType("has-decisionType")
                .hasRole(decisionTypeTarget).hasRole(decisionTypeValue);
        decisionType = graph.putResourceType("decisionType", ResourceType.DataType.STRING)
                .playsRole(decisionTypeValue);

        applicant = graph.putEntityType("applicant")
                .playsRole(TOEFLtarget)
                .playsRole(GREtarget)
                .playsRole(vGREtarget)
                .playsRole(GPRtarget)
                .playsRole(specialHonoursTarget)
                .playsRole(considerGPRtarget)
                .playsRole(transcriptTarget)
                .playsRole(priorGraduateWorkTarget)
                .playsRole(languageRequirementTarget)
                .playsRole(degreeOriginTarget)
                .playsRole(decisionTypeTarget)
                .playsRole(admissionStatusTarget);
        applicant.hasResource(key);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        Instance Alice = putEntity(graph, "Alice", applicant, key.getName());
        Instance Bob = putEntity(graph, "Bob", applicant, key.getName());
        Instance Charlie = putEntity(graph, "Charlie", applicant, key.getName());
        Instance Denis = putEntity(graph, "Denis", applicant, key.getName());
        Instance Eva = putEntity(graph, "Eva", applicant, key.getName());
        Instance Frank = putEntity(graph, "Frank", applicant, key.getName());

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
