package io.mindmaps.reasoner.graphs;


import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import io.mindmaps.graql.api.parser.QueryParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class AdmissionsGraph {

    private static MindmapsTransaction mindmaps;

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


    private static ResourceType<String> admissionStatus;
    private static ResourceType<String> decisionType;


    private static RelationType hasResource;

    private static RoleType hasResourceTarget, hasResourceValue;

    private AdmissionsGraph() {
    }

    public static MindmapsTransaction getTransaction() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.newTransaction();
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

        TOEFLtarget= mindmaps.putRoleType("TOEFL-owner");
        TOEFLvalue = mindmaps.putRoleType("TOEFL-value");
        TOEFLrelation = mindmaps.putRelationType("TOEFL-score")
                .hasRole(TOEFLtarget).hasRole(TOEFLvalue);
        TOEFL = mindmaps.putResourceType("TOEFL", Data.LONG).playsRole(hasResourceValue).playsRole(TOEFLvalue);

        GREtarget= mindmaps.putRoleType("GRE-owner");
        GREvalue = mindmaps.putRoleType("GRE-value");
        GRErelation = mindmaps.putRelationType("GRE-score")
                .hasRole(GREtarget).hasRole(GREvalue);
        GRE = mindmaps.putResourceType("GRE", Data.LONG).playsRole(hasResourceValue).playsRole(GREvalue);

        vGREtarget= mindmaps.putRoleType("verbal-GRE-owner");
        vGREvalue = mindmaps.putRoleType("verbal-GRE-value");
        vGRErelation = mindmaps.putRelationType("verbal-GRE-score")
                .hasRole(vGREtarget).hasRole(vGREvalue);
        vGRE = mindmaps.putResourceType("vGRE", Data.LONG).playsRole(hasResourceValue).playsRole(vGREvalue);

        GPRtarget= mindmaps.putRoleType("GPR-owner");
        GPRvalue = mindmaps.putRoleType("GPR-value");
        GPRrelation = mindmaps.putRelationType("GPR-score")
                .hasRole(GPRtarget).hasRole(GPRvalue);
        GPR = mindmaps.putResourceType("GPR", Data.DOUBLE).playsRole(hasResourceValue).playsRole(GPRvalue);

        specialHonoursTarget= mindmaps.putRoleType("special-honours-holder");
        specialHonoursValue = mindmaps.putRoleType("special-honours-type");
        specialHonoursRelation = mindmaps.putRelationType("special-honours")
                .hasRole(specialHonoursTarget).hasRole(specialHonoursValue);
        specialHonours = mindmaps.putResourceType("specialHonours", Data.STRING).playsRole(hasResourceValue)
                .playsRole(specialHonoursValue);

        considerGPRtarget= mindmaps.putRoleType("GPR-consideration-owner");
        considerGPRvalue = mindmaps.putRoleType("GPR-consideration-value");
        considerGPRrelation = mindmaps.putRelationType("GPR-consideration")
                .hasRole(considerGPRtarget).hasRole(considerGPRvalue);
        considerGPR = mindmaps.putResourceType("considerGPR", Data.STRING).playsRole(hasResourceValue).playsRole(considerGPRvalue);

        transcriptTarget = mindmaps.putRoleType("transcript-owner");
        transcriptValue = mindmaps.putRoleType("transcript-value");
        transcriptRelation = mindmaps.putRelationType("transcript-availability")
                .hasRole(transcriptTarget).hasRole(transcriptValue);
        transcript = mindmaps.putResourceType("transcript", Data.STRING).playsRole(hasResourceValue)
                .playsRole(transcriptValue);

        priorGraduateWorkTarget = mindmaps.putRoleType("PGW-owner");
        priorGraduateWorkValue = mindmaps.putRoleType("PGW-value");
        priorGraduateWorkRelation = mindmaps.putRelationType("prior-graduate-work")
                .hasRole(priorGraduateWorkTarget).hasRole(priorGraduateWorkValue);
        priorGraduateWork = mindmaps.putResourceType("priorGraduateWork", Data.STRING).playsRole(hasResourceValue)
                .playsRole(priorGraduateWorkValue);

        languageRequirementTarget = mindmaps.putRoleType("LR-owner");
        languageRequirementValue = mindmaps.putRoleType("LR-value");
        languageRequirementRelation = mindmaps.putRelationType("language-requirement")
                .hasRole(languageRequirementTarget).hasRole(languageRequirementValue);
        languageRequirement= mindmaps.putResourceType("languageRequirement", Data.STRING).playsRole(hasResourceValue)
                .playsRole(languageRequirementValue);

        degreeOriginTarget = mindmaps.putRoleType("degree-owner");
        degreeOriginValue = mindmaps.putRoleType("degree-origin-value");
        degreeOriginRelation = mindmaps.putRelationType("degree-origin")
                .hasRole(degreeOriginTarget).hasRole(degreeOriginValue);
        degreeOrigin = mindmaps.putResourceType("degreeOrigin", Data.STRING).playsRole(hasResourceValue)
                .playsRole(degreeOriginValue);

        applicant = mindmaps.putEntityType("applicant").setValue("applicant")
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
                .playsRole(degreeOriginTarget);


        admissionStatus = mindmaps.putResourceType("admissionStatus", Data.STRING).playsRole(hasResourceValue);
        decisionType = mindmaps.putResourceType("decisionType", Data.STRING).playsRole(hasResourceValue);


    }

    private static void buildInstances() {
        Instance Alice = putEntity(applicant, "Alice");
        Instance Bob = putEntity(applicant, "Bob");
        Instance Charlie = putEntity(applicant, "Charlie");
        Instance Denis = putEntity(applicant, "Denis");
        Instance Eva = putEntity(applicant, "Eva");
        Instance Frank = putEntity(applicant, "Frank");

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

    private static void addRules()
    {
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

    private static Instance putEntity(EntityType type, String name) {
        return mindmaps.putEntity(name.replaceAll(" ", "-").replaceAll("\\.", ""), type).setValue(name);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = mindmaps.putResource(UUID.randomUUID().toString(), resourceType).setValue(resource);

        mindmaps.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, instance)
                .putRolePlayer(hasResourceValue, resourceInstance);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource, RelationType relationType,
                                    RoleType targetRole, RoleType valueRole) {
        Resource resourceInstance = mindmaps.putResource(UUID.randomUUID().toString(), resourceType).setValue(resource);

        mindmaps.addRelation(relationType)
                .putRolePlayer(targetRole, instance)
                .putRolePlayer(valueRole, resourceInstance);
    }

}
