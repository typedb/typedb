/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.grakn.test.graql.reasoner.graphs;


import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.concept.EntityType;
import io.grakn.concept.Instance;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.exception.GraknValidationException;
import io.grakn.graql.Graql;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class AdmissionsGraph {

    private static GraknGraph grakn;

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

    private static ResourceType<String> considerGPR;
    private static RelationType considerGPRrelation;
    private static RoleType considerGPRvalue, considerGPRtarget;

    private static ResourceType<String> transcript;
    private static RelationType transcriptRelation;
    private static RoleType transcriptValue, transcriptTarget;

    private static ResourceType<String> priorGraduateWork;
    private static RelationType priorGraduateWorkRelation;
    private static RoleType priorGraduateWorkValue, priorGraduateWorkTarget;

    private static ResourceType<String> languageRequirement;
    private static RelationType languageRequirementRelation;
    private static RoleType languageRequirementValue, languageRequirementTarget;

    private static ResourceType<String> degreeOrigin;
    private static RelationType degreeOriginRelation;
    private static RoleType degreeOriginValue, degreeOriginTarget;


    private static RoleType admissionStatusTarget, admissionStatusValue;
    private static RelationType admissionStatusRelation;
    private static ResourceType<String> admissionStatus;

    private static RoleType decisionTypeTarget,  decisionTypeValue;
    private static RelationType decisionTypeRelation;
    private static ResourceType<String> decisionType;


    private static RelationType hasResource;

    private static RoleType hasResourceTarget, hasResourceValue;

    public static GraknGraph getGraph() {
        grakn = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        buildGraph();

        try {
            grakn.commit();
        } catch (GraknValidationException e) {
            System.out.println(e.getMessage());
        }

        return grakn;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        addRules();
    }

    private static void buildOntology() {
        hasResourceTarget = grakn.putRoleType("has-resource-target");
        hasResourceValue = grakn.putRoleType("has-resource-value");
        hasResource = grakn.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);

        TOEFLtarget= grakn.putRoleType("has-TOEFL-owner");
        TOEFLvalue = grakn.putRoleType("has-TOEFL-value");
        TOEFLrelation = grakn.putRelationType("has-TOEFL")
                .hasRole(TOEFLtarget).hasRole(TOEFLvalue);
        TOEFL = grakn.putResourceType("TOEFL", ResourceType.DataType.LONG).playsRole(hasResourceValue).playsRole(TOEFLvalue);

        GREtarget= grakn.putRoleType("has-GRE-owner");
        GREvalue = grakn.putRoleType("has-GRE-value");
        GRErelation = grakn.putRelationType("has-GRE")
                .hasRole(GREtarget).hasRole(GREvalue);
        GRE = grakn.putResourceType("GRE", ResourceType.DataType.LONG).playsRole(hasResourceValue).playsRole(GREvalue);

        vGREtarget= grakn.putRoleType("has-vGRE-owner");
        vGREvalue = grakn.putRoleType("has-vGRE-value");
        vGRErelation = grakn.putRelationType("has-vGRE")
                .hasRole(vGREtarget).hasRole(vGREvalue);
        vGRE = grakn.putResourceType("vGRE", ResourceType.DataType.LONG).playsRole(hasResourceValue).playsRole(vGREvalue);

        GPRtarget= grakn.putRoleType("has-GPR-owner");
        GPRvalue = grakn.putRoleType("has-GPR-value");
        GPRrelation = grakn.putRelationType("has-GPR")
                .hasRole(GPRtarget).hasRole(GPRvalue);
        GPR = grakn.putResourceType("GPR", ResourceType.DataType.DOUBLE).playsRole(hasResourceValue).playsRole(GPRvalue);

        specialHonoursTarget= grakn.putRoleType("has-specialHonours-owner");
        specialHonoursValue = grakn.putRoleType("has-specialHonours-value");
        specialHonoursRelation = grakn.putRelationType("has-specialHonours")
                .hasRole(specialHonoursTarget).hasRole(specialHonoursValue);
        specialHonours = grakn.putResourceType("specialHonours", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(specialHonoursValue);

        considerGPRtarget= grakn.putRoleType("has-considerGPR-owner");
        considerGPRvalue = grakn.putRoleType("has-considerGPR-value");
        considerGPRrelation = grakn.putRelationType("has-considerGPR")
                .hasRole(considerGPRtarget).hasRole(considerGPRvalue);
        considerGPR = grakn.putResourceType("considerGPR", ResourceType.DataType.STRING).playsRole(hasResourceValue).playsRole(considerGPRvalue);

        transcriptTarget = grakn.putRoleType("has-transcript-owner");
        transcriptValue = grakn.putRoleType("has-transcript-value");
        transcriptRelation = grakn.putRelationType("has-transcript")
                .hasRole(transcriptTarget).hasRole(transcriptValue);
        transcript = grakn.putResourceType("transcript", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(transcriptValue);

        priorGraduateWorkTarget = grakn.putRoleType("has-priorGraduateWork-owner");
        priorGraduateWorkValue = grakn.putRoleType("has-priorGraduateWork-value");
        priorGraduateWorkRelation = grakn.putRelationType("has-priorGraduateWork")
                .hasRole(priorGraduateWorkTarget).hasRole(priorGraduateWorkValue);
        priorGraduateWork = grakn.putResourceType("priorGraduateWork", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(priorGraduateWorkValue);

        languageRequirementTarget = grakn.putRoleType("has-languageRequirement-owner");
        languageRequirementValue = grakn.putRoleType("has-languageRequirement-value");
        languageRequirementRelation = grakn.putRelationType("has-languageRequirement")
                .hasRole(languageRequirementTarget).hasRole(languageRequirementValue);
        languageRequirement= grakn.putResourceType("languageRequirement", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(languageRequirementValue);

        degreeOriginTarget = grakn.putRoleType("has-degreeOrigin-owner");
        degreeOriginValue = grakn.putRoleType("has-degreeOrigin-value");
        degreeOriginRelation = grakn.putRelationType("has-degreeOrigin")
                .hasRole(degreeOriginTarget).hasRole(degreeOriginValue);
        degreeOrigin = grakn.putResourceType("degreeOrigin", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(degreeOriginValue);

        admissionStatusTarget = grakn.putRoleType("has-admissionStatus-owner");
        admissionStatusValue = grakn.putRoleType("has-admissionStatus-value");
        admissionStatusRelation = grakn.putRelationType("has-admissionStatus")
                .hasRole(admissionStatusTarget).hasRole(admissionStatusValue);
        admissionStatus = grakn.putResourceType("admissionStatus", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(admissionStatusValue);

        decisionTypeTarget = grakn.putRoleType("has-decisionType-owner");
        decisionTypeValue = grakn.putRoleType("has-decisionType-value");
        decisionTypeRelation = grakn.putRelationType("has-decisionType")
                .hasRole(decisionTypeTarget).hasRole(decisionTypeValue);
        decisionType = grakn.putResourceType("decisionType", ResourceType.DataType.STRING).playsRole(hasResourceValue)
                .playsRole(decisionTypeValue);

        applicant = grakn.putEntityType("applicant")
                .playsRole(hasResourceTarget)
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


    }

    private static void buildInstances() {
        Instance Alice = grakn.putEntity("Alice", applicant);
        Instance Bob = grakn.putEntity("Bob", applicant);
        Instance Charlie = grakn.putEntity("Charlie", applicant);
        Instance Denis = grakn.putEntity("Denis", applicant);
        Instance Eva = grakn.putEntity("Eva", applicant);
        Instance Frank = grakn.putEntity("Frank", applicant);

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

    private static void addRules() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/graql/admission-rules.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            Graql.withGraph(grakn).parse(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                    RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = grakn.putResource(resource, resourceType);

        grakn.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }

}
