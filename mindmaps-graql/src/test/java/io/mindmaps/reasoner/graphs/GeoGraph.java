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

package io.mindmaps.reasoner.graphs;

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;

import java.util.UUID;

public class GeoGraph {

    private static MindmapsTransaction mindmaps;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType hasResource, isLocatedIn;

    private static RoleType geoEntity, location;
    private static RoleType hasResourceTarget, hasResourceValue;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

    private GeoGraph() {
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
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {

        hasResourceTarget = mindmaps.putRoleType("has-resource-target");
        hasResourceValue = mindmaps.putRoleType("has-resource-value");
        hasResource = mindmaps.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);


        geoEntity = mindmaps.putRoleType("geoEntity");
        location = mindmaps.putRoleType("location");
        isLocatedIn = mindmaps.putRelationType("isLocatedIn")
                .hasRole(geoEntity).hasRole(location);

        geographicalObject = mindmaps.putEntityType("geoObject").setValue("geoObject");

        continent = mindmaps.putEntityType("continent").setValue("continent").superType(geographicalObject)
                    .playsRole(location);
        country = mindmaps.putEntityType("country").setValue("country").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(location);
        region = mindmaps.putEntityType("region").setValue("region").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(location);
        city = mindmaps.putEntityType("city").setValue("city").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(location);
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
                .putRolePlayer(location, Warsaw);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UW)
                .putRolePlayer(location, Warsaw);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Imperial)
                .putRolePlayer(location, London);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UCL)
                .putRolePlayer(location, London);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Warsaw)
                .putRolePlayer(location, Masovia);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Masovia)
                .putRolePlayer(location, Poland);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Wroclaw)
                .putRolePlayer(location, Silesia);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Silesia)
                .putRolePlayer(location, Poland);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Poland)
                .putRolePlayer(location, Europe);



        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, London)
                .putRolePlayer(location, GreaterLondon);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, GreaterLondon)
                .putRolePlayer(location, England);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, England)
                .putRolePlayer(location, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Munich)
                .putRolePlayer(location, Bavaria);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Bavaria)
                .putRolePlayer(location, Germany);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Germany)
                .putRolePlayer(location, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Milan)
                .putRolePlayer(location, Lombardy);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Lombardy)
                .putRolePlayer(location, Italy);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Italy)
                .putRolePlayer(location, Europe);

        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Paris)
                .putRolePlayer(location, IleDeFrance);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, IleDeFrance)
                .putRolePlayer(location, France);
        mindmaps.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, France)
                .putRolePlayer(location, Europe);

    }
    private static void buildRules() {

        RuleType inferenceRule = mindmaps.getMetaRuleInference();

        Rule transitivity = mindmaps.putRule("transitivity", inferenceRule);

        String transitivity_LHS = "match " +
                "($x, $y) isa isLocatedIn;\n" +
                "($y, $z) isa isLocatedIn; select $x, $z";

        String transitivity_RHS = "match " +
                "($x, $z) isa isLocatedIn select $x, $z";

        transitivity.setLHS(transitivity_LHS);
        transitivity.setRHS(transitivity_RHS);

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
