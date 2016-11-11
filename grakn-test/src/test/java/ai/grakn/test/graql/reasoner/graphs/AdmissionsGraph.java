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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RoleType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class AdmissionsGraph extends TestGraph{

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

    public static GraknGraph getGraph() {
        return new AdmissionsGraph().graph();
    }

    @Override
    protected void buildOntology() {
        TOEFLtarget= graknGraph.putRoleType("has-TOEFL-owner");
        TOEFLvalue = graknGraph.putRoleType("has-TOEFL-value");
        TOEFLrelation = graknGraph.putRelationType("has-TOEFL")
                .hasRole(TOEFLtarget).hasRole(TOEFLvalue);
        TOEFL = graknGraph.putResourceType("TOEFL", ResourceType.DataType.LONG).playsRole(TOEFLvalue);

        GREtarget= graknGraph.putRoleType("has-GRE-owner");
        GREvalue = graknGraph.putRoleType("has-GRE-value");
        GRErelation = graknGraph.putRelationType("has-GRE")
                .hasRole(GREtarget).hasRole(GREvalue);
        GRE = graknGraph.putResourceType("GRE", ResourceType.DataType.LONG).playsRole(GREvalue);

        vGREtarget= graknGraph.putRoleType("has-vGRE-owner");
        vGREvalue = graknGraph.putRoleType("has-vGRE-value");
        vGRErelation = graknGraph.putRelationType("has-vGRE")
                .hasRole(vGREtarget).hasRole(vGREvalue);
        vGRE = graknGraph.putResourceType("vGRE", ResourceType.DataType.LONG).playsRole(vGREvalue);

        GPRtarget= graknGraph.putRoleType("has-GPR-owner");
        GPRvalue = graknGraph.putRoleType("has-GPR-value");
        GPRrelation = graknGraph.putRelationType("has-GPR")
                .hasRole(GPRtarget).hasRole(GPRvalue);
        GPR = graknGraph.putResourceType("GPR", ResourceType.DataType.DOUBLE).playsRole(GPRvalue);

        specialHonoursTarget= graknGraph.putRoleType("has-specialHonours-owner");
        specialHonoursValue = graknGraph.putRoleType("has-specialHonours-value");
        specialHonoursRelation = graknGraph.putRelationType("has-specialHonours")
                .hasRole(specialHonoursTarget).hasRole(specialHonoursValue);
        specialHonours = graknGraph.putResourceType("specialHonours", ResourceType.DataType.STRING)
                .playsRole(specialHonoursValue);

        considerGPRtarget= graknGraph.putRoleType("has-considerGPR-owner");
        considerGPRvalue = graknGraph.putRoleType("has-considerGPR-value");
        considerGPRrelation = graknGraph.putRelationType("has-considerGPR")
                .hasRole(considerGPRtarget).hasRole(considerGPRvalue);
        considerGPR = graknGraph.putResourceType("considerGPR", ResourceType.DataType.STRING).playsRole(considerGPRvalue);

        transcriptTarget = graknGraph.putRoleType("has-transcript-owner");
        transcriptValue = graknGraph.putRoleType("has-transcript-value");
        transcriptRelation = graknGraph.putRelationType("has-transcript")
                .hasRole(transcriptTarget).hasRole(transcriptValue);
        transcript = graknGraph.putResourceType("transcript", ResourceType.DataType.STRING)
                .playsRole(transcriptValue);

        priorGraduateWorkTarget = graknGraph.putRoleType("has-priorGraduateWork-owner");
        priorGraduateWorkValue = graknGraph.putRoleType("has-priorGraduateWork-value");
        priorGraduateWorkRelation = graknGraph.putRelationType("has-priorGraduateWork")
                .hasRole(priorGraduateWorkTarget).hasRole(priorGraduateWorkValue);
        priorGraduateWork = graknGraph.putResourceType("priorGraduateWork", ResourceType.DataType.STRING)
                .playsRole(priorGraduateWorkValue);

        languageRequirementTarget = graknGraph.putRoleType("has-languageRequirement-owner");
        languageRequirementValue = graknGraph.putRoleType("has-languageRequirement-value");
        languageRequirementRelation = graknGraph.putRelationType("has-languageRequirement")
                .hasRole(languageRequirementTarget).hasRole(languageRequirementValue);
        languageRequirement= graknGraph.putResourceType("languageRequirement", ResourceType.DataType.STRING)
                .playsRole(languageRequirementValue);

        degreeOriginTarget = graknGraph.putRoleType("has-degreeOrigin-owner");
        degreeOriginValue = graknGraph.putRoleType("has-degreeOrigin-value");
        degreeOriginRelation = graknGraph.putRelationType("has-degreeOrigin")
                .hasRole(degreeOriginTarget).hasRole(degreeOriginValue);
        degreeOrigin = graknGraph.putResourceType("degreeOrigin", ResourceType.DataType.STRING)
                .playsRole(degreeOriginValue);

        admissionStatusTarget = graknGraph.putRoleType("has-admissionStatus-owner");
        admissionStatusValue = graknGraph.putRoleType("has-admissionStatus-value");
        admissionStatusRelation = graknGraph.putRelationType("has-admissionStatus")
                .hasRole(admissionStatusTarget).hasRole(admissionStatusValue);
        admissionStatus = graknGraph.putResourceType("admissionStatus", ResourceType.DataType.STRING)
                .playsRole(admissionStatusValue);

        decisionTypeTarget = graknGraph.putRoleType("has-decisionType-owner");
        decisionTypeValue = graknGraph.putRoleType("has-decisionType-value");
        decisionTypeRelation = graknGraph.putRelationType("has-decisionType")
                .hasRole(decisionTypeTarget).hasRole(decisionTypeValue);
        decisionType = graknGraph.putResourceType("decisionType", ResourceType.DataType.STRING)
                .playsRole(decisionTypeValue);

        applicant = graknGraph.putEntityType("applicant")
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
                .playsRole(admissionStatusTarget)
                .playsRole(hasKeyTarget);
    }

    @Override
    protected void buildInstances() {
        Instance Alice = putEntity("Alice", applicant);
        Instance Bob = putEntity("Bob", applicant);
        Instance Charlie = putEntity("Charlie", applicant);
        Instance Denis = putEntity("Denis", applicant);
        Instance Eva = putEntity("Eva", applicant);
        Instance Frank = putEntity("Frank", applicant);

        putResource(Alice, TOEFL, 470L, TOEFLrelation,TOEFLtarget, TOEFLvalue);
        putResource(Alice, degreeOrigin, "nonUS", degreeOriginRelation, degreeOriginTarget, degreeOriginValue);

        putResource(Bob, priorGraduateWork, "none", priorGraduateWorkRelation, priorGraduateWorkTarget, priorGraduateWorkValue);
        putResource(Bob, TOEFL, 520L, TOEFLrelation,TOEFLtarget, TOEFLvalue);
        putResource(Bob, degreeOrigin, "US", degreeOriginRelation, degreeOriginTarget, degreeOriginValue);
        putResource(Bob, transcript, "unavailable", transcriptRelation, transcriptTarget, transcriptValue);
        putResource(Bob, specialHonours, "none", specialHonoursRelation, specialHonoursTarget, specialHonoursValue);
        putResource(Bob, GRE, 1100L, GRErelation, GREtarget, GREvalue);

        putResource(Charlie, priorGraduateWork, "none", priorGraduateWorkRelation, priorGraduateWorkTarget, priorGraduateWorkValue);
        putResource(Charlie, TOEFL, 600L, TOEFLrelation,TOEFLtarget, TOEFLvalue);
        putResource(Charlie, degreeOrigin, "US", degreeOriginRelation, degreeOriginTarget, degreeOriginValue);
        putResource(Charlie, transcript, "available", transcriptRelation, transcriptTarget, transcriptValue);
        putResource(Charlie, specialHonours, "none", specialHonoursRelation, specialHonoursTarget, specialHonoursValue);
        putResource(Charlie, GRE, 1100L, GRErelation, GREtarget, GREvalue);
        putResource(Charlie, vGRE, 400L, vGRErelation, vGREtarget, vGREvalue);
        putResource(Charlie, GPR, 2.99, GPRrelation, GPRtarget, GPRvalue);

        putResource(Denis, priorGraduateWork, "none", priorGraduateWorkRelation, priorGraduateWorkTarget, priorGraduateWorkValue);
        putResource(Denis, degreeOrigin, "US", degreeOriginRelation, degreeOriginTarget, degreeOriginValue);
        putResource(Denis, transcript, "available", transcriptRelation, transcriptTarget, transcriptValue);
        putResource(Denis, specialHonours, "none", specialHonoursRelation, specialHonoursTarget, specialHonoursValue);
        putResource(Denis, GRE, 900L, GRErelation, GREtarget, GREvalue);
        putResource(Denis, vGRE, 350L, vGRErelation, vGREtarget, vGREvalue);
        putResource(Denis, GPR, 2.5, GPRrelation, GPRtarget, GPRvalue);

        putResource(Eva, priorGraduateWork, "completed", priorGraduateWorkRelation, priorGraduateWorkTarget, priorGraduateWorkValue);
        putResource(Eva, specialHonours, "valedictorian", specialHonoursRelation, specialHonoursTarget, specialHonoursValue);
        putResource(Eva, GPR, 3.0, GPRrelation, GPRtarget, GPRvalue);

        putResource(Frank, TOEFL, 550L, TOEFLrelation,TOEFLtarget, TOEFLvalue);
        putResource(Frank, degreeOrigin, "US", degreeOriginRelation, degreeOriginTarget, degreeOriginValue);
        putResource(Frank, transcript, "unavailable", transcriptRelation, transcriptTarget, transcriptValue);
        putResource(Frank, specialHonours, "none", specialHonoursRelation, specialHonoursTarget, specialHonoursValue);
        putResource(Frank, GRE, 100L, GRErelation, GREtarget, GREvalue);
    }

    @Override
    protected void buildRules() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/graql/admission-rules.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            graknGraph.graql().parse(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}
