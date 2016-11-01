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

package ai.grakn.test.graql.reasoner.graphs;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;

import java.util.UUID;

public class GeoGraph {

    private static GraknGraph grakn;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType hasResource, isLocatedIn;

    private static RoleType geoEntity, entityLocation;
    private static RoleType hasResourceTarget, hasResourceValue;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

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
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {
        hasResourceTarget = grakn.putRoleType("has-resource-target");
        hasResourceValue = grakn.putRoleType("has-resource-value");
        hasResource = grakn.putRelationType("has-resource")
                .hasRole(hasResourceTarget).hasRole(hasResourceValue);


        geoEntity = grakn.putRoleType("geo-entity");
        entityLocation = grakn.putRoleType("entity-location");
        isLocatedIn = grakn.putRelationType("is-located-in")
                .hasRole(geoEntity).hasRole(entityLocation);

        geographicalObject = grakn.putEntityType("geoObject");

        continent = grakn.putEntityType("continent").superType(geographicalObject)
                    .playsRole(entityLocation);
        country = grakn.putEntityType("country").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        region = grakn.putEntityType("region").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        city = grakn.putEntityType("city").superType(geographicalObject)
                .playsRole(geoEntity).playsRole(entityLocation);
        university = grakn.putEntityType("university")
                        .playsRole(geoEntity);
    }

    private static void buildInstances() {

        Europe = grakn.putEntity("Europe", continent);
        NorthAmerica = grakn.putEntity("Europe", continent);

        Poland = grakn.putEntity("Poland", country);
        England = grakn.putEntity("England", country);
        Germany = grakn.putEntity("Germany", country);
        France = grakn.putEntity("France", country);
        Italy = grakn.putEntity("Italy", country);

        Masovia = grakn.putEntity("Masovia", region);
        Silesia = grakn.putEntity("Silesia", region);
        GreaterLondon = grakn.putEntity("GreaterLondon", region);
        Bavaria = grakn.putEntity("Bavaria", region);
        IleDeFrance = grakn.putEntity("IleDeFrance", region);
        Lombardy = grakn.putEntity("Lombardy", region);

        Warsaw = grakn.putEntity("Warsaw", city);
        Wroclaw = grakn.putEntity("Wroclaw", city);
        London = grakn.putEntity("London", city);
        Munich = grakn.putEntity("Munich", city);
        Paris = grakn.putEntity("Paris", city);
        Milan = grakn.putEntity("Milan", city);

        UW = grakn.putEntity("University-of-Warsaw", university);
        PW = grakn.putEntity("Warsaw-Polytechnics", university);
        Imperial = grakn.putEntity("Imperial College London", university);
        UCL = grakn.putEntity("University College London", university);
        UniversityOfMunich = grakn.putEntity("University of Munich", university);

    }

    private static void buildRelations() {
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, PW)
                .putRolePlayer(entityLocation, Warsaw);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UW)
                .putRolePlayer(entityLocation, Warsaw);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Imperial)
                .putRolePlayer(entityLocation, London);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UCL)
                .putRolePlayer(entityLocation, London);

        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Warsaw)
                .putRolePlayer(entityLocation, Masovia);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Masovia)
                .putRolePlayer(entityLocation, Poland);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Wroclaw)
                .putRolePlayer(entityLocation, Silesia);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Silesia)
                .putRolePlayer(entityLocation, Poland);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Poland)
                .putRolePlayer(entityLocation, Europe);

        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, London)
                .putRolePlayer(entityLocation, GreaterLondon);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, GreaterLondon)
                .putRolePlayer(entityLocation, England);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, England)
                .putRolePlayer(entityLocation, Europe);

        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Munich)
                .putRolePlayer(entityLocation, Bavaria);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Bavaria)
                .putRolePlayer(entityLocation, Germany);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Germany)
                .putRolePlayer(entityLocation, Europe);

        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Milan)
                .putRolePlayer(entityLocation, Lombardy);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Lombardy)
                .putRolePlayer(entityLocation, Italy);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Italy)
                .putRolePlayer(entityLocation, Europe);

        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Paris)
                .putRolePlayer(entityLocation, IleDeFrance);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, IleDeFrance)
                .putRolePlayer(entityLocation, France);
        grakn.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, France)
                .putRolePlayer(entityLocation, Europe);

    }
    private static void buildRules() {
        RuleType inferenceRule = grakn.getMetaRuleInference();

        String transitivity_LHS = "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;";

        String transitivity_RHS = "(geo-entity: $x, entity-location: $z) isa is-located-in;";

        grakn.putRule("transitivity", transitivity_LHS, transitivity_RHS, inferenceRule);
    }

    private static <T> void putResource(Instance instance, ResourceType<T> resourceType, T resource) {
        Resource resourceInstance = grakn.putResource(resource, resourceType);

        grakn.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, instance)
                .putRolePlayer(hasResourceValue, resourceInstance);
    }
}
