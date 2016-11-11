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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.graql.Pattern;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;

import static ai.grakn.graql.Graql.and;

public class GeoGraph extends TestGraph{

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType isLocatedIn;

    private static RoleType geoEntity, entityLocation;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

    public static GraknGraph getGraph() {
        return new GeoGraph().graph();
    }

    @Override
    protected void buildOntology() {
        geoEntity = graknGraph.putRoleType("geo-entity");
        entityLocation = graknGraph.putRoleType("entity-location");
        isLocatedIn = graknGraph.putRelationType("is-located-in")
                .hasRole(geoEntity).hasRole(entityLocation);

        geographicalObject = graknGraph.putEntityType("geoObject")
                .playsRole(hasKeyTarget)
                .playsRole(geoEntity)
                .playsRole(entityLocation);

        continent = graknGraph.putEntityType("continent")
                .superType(geographicalObject);
        country = graknGraph.putEntityType("country")
                .superType(geographicalObject);
        region = graknGraph.putEntityType("region")
                .superType(geographicalObject);
        city = graknGraph.putEntityType("city")
                .superType(geographicalObject);
        university = graknGraph.putEntityType("university")
                        .playsRole(geoEntity)
                        .playsRole(hasKeyTarget);
    }

    @Override
    protected void buildInstances() {
        Europe = putEntity("Europe", continent);
        NorthAmerica = putEntity("NorthAmerica", continent);

        Poland = putEntity("Poland", country);
        England = putEntity("England", country);
        Germany = putEntity("Germany", country);
        France = putEntity("France", country);
        Italy = putEntity("Italy", country);

        Masovia = putEntity("Masovia", region);
        Silesia = putEntity("Silesia", region);
        GreaterLondon = putEntity("GreaterLondon", region);
        Bavaria = putEntity("Bavaria", region);
        IleDeFrance = putEntity("IleDeFrance", region);
        Lombardy = putEntity("Lombardy", region);

        Warsaw = putEntity("Warsaw", city);
        Wroclaw = putEntity("Wroclaw", city);
        London = putEntity("London", city);
        Munich = putEntity("Munich", city);
        Paris = putEntity("Paris", city);
        Milan = putEntity("Milan", city);

        UW = putEntity("University-of-Warsaw", university);
        PW = putEntity("Warsaw-Polytechnics", university);
        Imperial = putEntity("Imperial College London", university);
        UCL = putEntity("University College London", university);
        UniversityOfMunich = putEntity("University of Munich", university);
    }

    @Override
    protected void buildRelations() {
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, PW)
                .putRolePlayer(entityLocation, Warsaw);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UW)
                .putRolePlayer(entityLocation, Warsaw);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Imperial)
                .putRolePlayer(entityLocation, London);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, UCL)
                .putRolePlayer(entityLocation, London);

        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Warsaw)
                .putRolePlayer(entityLocation, Masovia);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Masovia)
                .putRolePlayer(entityLocation, Poland);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Wroclaw)
                .putRolePlayer(entityLocation, Silesia);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Silesia)
                .putRolePlayer(entityLocation, Poland);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Poland)
                .putRolePlayer(entityLocation, Europe);

        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, London)
                .putRolePlayer(entityLocation, GreaterLondon);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, GreaterLondon)
                .putRolePlayer(entityLocation, England);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, England)
                .putRolePlayer(entityLocation, Europe);

        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Munich)
                .putRolePlayer(entityLocation, Bavaria);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Bavaria)
                .putRolePlayer(entityLocation, Germany);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Germany)
                .putRolePlayer(entityLocation, Europe);

        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Milan)
                .putRolePlayer(entityLocation, Lombardy);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Lombardy)
                .putRolePlayer(entityLocation, Italy);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Italy)
                .putRolePlayer(entityLocation, Europe);

        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, Paris)
                .putRolePlayer(entityLocation, IleDeFrance);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, IleDeFrance)
                .putRolePlayer(entityLocation, France);
        graknGraph.addRelation(isLocatedIn)
                .putRolePlayer(geoEntity, France)
                .putRolePlayer(entityLocation, Europe);
    }

    @Override
    protected void buildRules() {
        RuleType inferenceRule = graknGraph.getMetaRuleInference();
        Pattern transitivity_LHS = and(graknGraph.graql().parsePatterns(
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;"));
        Pattern transitivity_RHS = and(graknGraph.graql().parsePatterns("(geo-entity: $x, entity-location: $z) isa is-located-in;"));
        graknGraph.addRule(transitivity_LHS, transitivity_RHS, inferenceRule);
    }
}
