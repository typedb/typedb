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

import io.mindmaps.MindmapsTransaction;
import io.mindmaps.core.MindmapsGraph;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;

import java.util.UUID;

public class GeoGraph {

    private static MindmapsTransaction mindmaps;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType hasResource, isLocatedIn;

    private static RoleType geoEntity, entityLocation;
    private static RoleType hasResourceTarget, hasResourceValue;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

    public static MindmapsTransaction getTransaction() {
        MindmapsGraph graph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmaps = graph.getTransaction();
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
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {
        hasResourceTarget = mindmaps.putRoleType("has-resource-target");
        hasResourceValue = mindmaps.putRoleType("has-resource-value");
        hasResource = mindmaps.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);


        geoEntity = mindmaps.putRoleType("geo-entity");
        entityLocation = mindmaps.putRoleType("entity-location");
        isLocatedIn = mindmaps.putRelationType("is-located-in")
                .hasRole(geoEntity).hasRole(entityLocation);

        geographicalObject = mindmaps.putEntityType("geoObject").setValue("geoObject");

        continent = mindmaps.putEntityType("continent").setValue("continent").superType(geographicalObject)
                    .playsRole(entityLocation);
        country = mindmaps.putEntityType("country").setValue("country").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        region = mindmaps.putEntityType("region").setValue("region").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        city = mindmaps.putEntityType("city").setValue("city").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        university = mindmaps.putEntityType("university").setValue("university")
                        .playsRole(geoEntity);
    }

    private static void buildInstances() {

        Europe = putEntity(continent, "Europe");
        NorthAmerica = putEntity(continent, "Europe");

        Poland = putEntity(country, "Poland");
        England = putEntity(country, "England");
        Germany = putEntity(country, "Germany");
        France = putEntity(country, "France");
        Italy = putEntity(country, "Italy");

        Masovia = putEntity(region, "Masovia");
        Silesia = putEntity(region, "Silesia");
        GreaterLondon = putEntity(region, "GreaterLondon");
        Bavaria = putEntity(region, "Bavaria");
        IleDeFrance = putEntity(region, "IleDeFrance");
        Lombardy = putEntity(region, "Lombardy");

        Warsaw = putEntity(city, "Warsaw");
        Wroclaw = putEntity(city, "Wroclaw");
        London = putEntity(city, "London");
        Munich = putEntity(city, "Munich");
        Paris = putEntity(city, "Paris");
        Milan = putEntity(city, "Milan");

        UW = putEntity(university, "University of Warsaw");
        PW = putEntity(university, "Warsaw Polytechnics");
        Imperial = putEntity(university, "Imperial College London");
        UCL = putEntity(university, "University College London");
        UniversityOfMunich = putEntity(university, "University of Munich");

    }

    private static void buildRelations() {
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, PW)
                .putRolePlayer(entityLocation, Warsaw);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UW)
                .putRolePlayer(entityLocation, Warsaw);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Imperial)
                .putRolePlayer(entityLocation, London);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UCL)
                .putRolePlayer(entityLocation, London);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Warsaw)
                .putRolePlayer(entityLocation, Masovia);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Masovia)
                .putRolePlayer(entityLocation, Poland);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Wroclaw)
                .putRolePlayer(entityLocation, Silesia);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Silesia)
                .putRolePlayer(entityLocation, Poland);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Poland)
                .putRolePlayer(entityLocation, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, London)
                .putRolePlayer(entityLocation, GreaterLondon);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, GreaterLondon)
                .putRolePlayer(entityLocation, England);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, England)
                .putRolePlayer(entityLocation, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Munich)
                .putRolePlayer(entityLocation, Bavaria);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Bavaria)
                .putRolePlayer(entityLocation, Germany);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Germany)
                .putRolePlayer(entityLocation, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Milan)
                .putRolePlayer(entityLocation, Lombardy);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Lombardy)
                .putRolePlayer(entityLocation, Italy);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Italy)
                .putRolePlayer(entityLocation, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Paris)
                .putRolePlayer(entityLocation, IleDeFrance);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, IleDeFrance)
                .putRolePlayer(entityLocation, France);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, France)
                .putRolePlayer(entityLocation, Europe);

    }
    private static void buildRules() {
        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        String transitivity_LHS = "match " +
                "(geo-entity $x, entity-location $y) isa is-located-in;\n" +
                "(geo-entity $y, entity-location $z) isa is-located-in; select $x, $z";

        String transitivity_RHS = "match " +
                "(geo-entity $x, entity-location $z) isa is-located-in select $x, $z";

        mindmaps.putRule("transitivity", transitivity_LHS, transitivity_RHS, inferenceRule);
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
}
