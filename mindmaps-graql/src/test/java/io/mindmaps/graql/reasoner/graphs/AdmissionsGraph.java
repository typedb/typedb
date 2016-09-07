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

package io.mindmaps.graql.reasoner.graphs;


import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.Data;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.QueryParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class AdmissionsGraph {

    private static MindmapsGraph mindmaps;

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

    public static MindmapsGraph getGraph() {
        mindmaps = MindmapsTestGraphFactory.newEmptyGraph();
        buildGraph();

        try {
            mindmaps.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }

        return mindmaps;
    }

    private static void buildGraph() {
        buildOntology();
        buildInstances();
        addRules();
    }

    private static void buildOntology() {
        hasResourceTarget = mindmaps.putRoleType("has-resource-target");
        hasResourceValue = mindmaps.putRoleType("has-resource-value");
        hasResource = mindmaps.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);

        TOEFLtarget= mindmaps.putRoleType("has-TOEFL-owner");
        TOEFLvalue = mindmaps.putRoleType("has-TOEFL-value");
        TOEFLrelation = mindmaps.putRelationType("has-TOEFL")
                .hasRole(TOEFLtarget).hasRole(TOEFLvalue);
        TOEFL = mindmaps.putResourceType("TOEFL", Data.LONG).playsRole(hasResourceValue).playsRole(TOEFLvalue);

        GREtarget= mindmaps.putRoleType("has-GRE-owner");
        GREvalue = mindmaps.putRoleType("has-GRE-value");
        GRErelation = mindmaps.putRelationType("has-GRE")
                .hasRole(GREtarget).hasRole(GREvalue);
        GRE = mindmaps.putResourceType("GRE", Data.LONG).playsRole(hasResourceValue).playsRole(GREvalue);

        vGREtarget= mindmaps.putRoleType("has-vGRE-owner");
        vGREvalue = mindmaps.putRoleType("has-vGRE-value");
        vGRErelation = mindmaps.putRelationType("has-vGRE")
                .hasRole(vGREtarget).hasRole(vGREvalue);
        vGRE = mindmaps.putResourceType("vGRE", Data.LONG).playsRole(hasResourceValue).playsRole(vGREvalue);

        GPRtarget= mindmaps.putRoleType("has-GPR-owner");
        GPRvalue = mindmaps.putRoleType("has-GPR-value");
        GPRrelation = mindmaps.putRelationType("has-GPR")
                .hasRole(GPRtarget).hasRole(GPRvalue);
        GPR = mindmaps.putResourceType("GPR", Data.DOUBLE).playsRole(hasResourceValue).playsRole(GPRvalue);

        specialHonoursTarget= mindmaps.putRoleType("has-specialHonours-owner");
        specialHonoursValue = mindmaps.putRoleType("has-specialHonours-value");
        specialHonoursRelation = mindmaps.putRelationType("has-specialHonours")
                .hasRole(specialHonoursTarget).hasRole(specialHonoursValue);
        specialHonours = mindmaps.putResourceType("specialHonours", Data.STRING).playsRole(hasResourceValue)
                .playsRole(specialHonoursValue);

        considerGPRtarget= mindmaps.putRoleType("has-considerGPR-owner");
        considerGPRvalue = mindmaps.putRoleType("has-considerGPR-value");
        considerGPRrelation = mindmaps.putRelationType("has-considerGPR")
                .hasRole(considerGPRtarget).hasRole(considerGPRvalue);
        considerGPR = mindmaps.putResourceType("considerGPR", Data.STRING).playsRole(hasResourceValue).playsRole(considerGPRvalue);

        transcriptTarget = mindmaps.putRoleType("has-transcript-owner");
        transcriptValue = mindmaps.putRoleType("has-transcript-value");
        transcriptRelation = mindmaps.putRelationType("has-transcript")
                .hasRole(transcriptTarget).hasRole(transcriptValue);
        transcript = mindmaps.putResourceType("transcript", Data.STRING).playsRole(hasResourceValue)
                .playsRole(transcriptValue);

        priorGraduateWorkTarget = mindmaps.putRoleType("has-priorGraduateWork-owner");
        priorGraduateWorkValue = mindmaps.putRoleType("has-priorGraduateWork-value");
        priorGraduateWorkRelation = mindmaps.putRelationType("has-priorGraduateWork")
                .hasRole(priorGraduateWorkTarget).hasRole(priorGraduateWorkValue);
        priorGraduateWork = mindmaps.putResourceType("priorGraduateWork", Data.STRING).playsRole(hasResourceValue)
                .playsRole(priorGraduateWorkValue);

        languageRequirementTarget = mindmaps.putRoleType("has-languageRequirement-owner");
        languageRequirementValue = mindmaps.putRoleType("has-languageRequirement-value");
        languageRequirementRelation = mindmaps.putRelationType("has-languageRequirement")
                .hasRole(languageRequirementTarget).hasRole(languageRequirementValue);
        languageRequirement= mindmaps.putResourceType("languageRequirement", Data.STRING).playsRole(hasResourceValue)
                .playsRole(languageRequirementValue);

        degreeOriginTarget = mindmaps.putRoleType("has-degreeOrigin-owner");
        degreeOriginValue = mindmaps.putRoleType("has-degreeOrigin-value");
        degreeOriginRelation = mindmaps.putRelationType("has-degreeOrigin")
                .hasRole(degreeOriginTarget).hasRole(degreeOriginValue);
        degreeOrigin = mindmaps.putResourceType("degreeOrigin", Data.STRING).playsRole(hasResourceValue)
                .playsRole(degreeOriginValue);

        admissionStatusTarget = mindmaps.putRoleType("has-admissionStatus-owner");
        admissionStatusValue = mindmaps.putRoleType("has-admissionStatus-value");
        admissionStatusRelation = mindmaps.putRelationType("has-admissionStatus")
                .hasRole(admissionStatusTarget).hasRole(admissionStatusValue);
        admissionStatus = mindmaps.putResourceType("admissionStatus", Data.STRING).playsRole(hasResourceValue)
                .playsRole(admissionStatusValue);

        decisionTypeTarget = mindmaps.putRoleType("has-decisionType-owner");
        decisionTypeValue = mindmaps.putRoleType("has-decisionType-value");
        decisionTypeRelation = mindmaps.putRelationType("has-decisionType")
                .hasRole(decisionTypeTarget).hasRole(decisionTypeValue);
        decisionType = mindmaps.putResourceType("decisionType", Data.STRING).playsRole(hasResourceValue)
                .playsRole(decisionTypeValue);

        applicant = mindmaps.putEntityType("applicant")
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
        Instance Alice = mindmaps.putEntity("Alice", applicant);
        Instance Bob = mindmaps.putEntity("Bob", applicant);
        Instance Charlie = mindmaps.putEntity("Charlie", applicant);
        Instance Denis = mindmaps.putEntity("Denis", applicant);
        Instance Eva = mindmaps.putEntity("Eva", applicant);
        Instance Frank = mindmaps.putEntity("Frank", applicant);

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
        QueryParser qp = QueryParser.create(mindmaps);
        try {
            List<String> lines = Files.readAllLines(Paths.get("src/test/resources/graql/admission-rules.gql"), StandardCharsets.UTF_8);
            String query = lines.stream().reduce("", (s1, s2) -> s1 + "\n" + s2);
            qp.parseInsertQuery(query).execute();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                    RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = mindmaps.putResource(resource, resourceType);

        mindmaps.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }

}
