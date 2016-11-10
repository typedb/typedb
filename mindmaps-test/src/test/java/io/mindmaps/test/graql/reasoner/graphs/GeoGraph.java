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

package io.mindmaps.test.graql.reasoner.graphs;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.graql.Pattern;

import static io.mindmaps.graql.Graql.and;

public class GeoGraph extends TestGraph{

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType isLocatedIn;

    private static RoleType geoEntity, entityLocation;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

    public static MindmapsGraph getGraph() {
        return new GeoGraph().graph();
    }

    @Override
    protected void buildOntology() {
        geoEntity = mindmaps.putRoleType("geo-entity");
        entityLocation = mindmaps.putRoleType("entity-location");
        isLocatedIn = mindmaps.putRelationType("is-located-in")
                .hasRole(geoEntity).hasRole(entityLocation);

        geographicalObject = mindmaps.putEntityType("geoObject").playsRole(hasKeyTarget);

        continent = mindmaps.putEntityType("continent")
                .superType(geographicalObject)
                .playsRole(entityLocation);
        country = mindmaps.putEntityType("country")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        region = mindmaps.putEntityType("region")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        city = mindmaps.putEntityType("city")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        university = mindmaps.putEntityType("university")
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

    @Override
    protected void buildRules() {
        RuleType inferenceRule = mindmaps.getMetaRuleInference();
        Pattern transitivity_LHS = and(mindmaps.graql().parsePatterns(
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;"));
        Pattern transitivity_RHS = and(mindmaps.graql().parsePatterns("(geo-entity: $x, entity-location: $z) isa is-located-in;"));
        mindmaps.addRule(transitivity_LHS, transitivity_RHS, inferenceRule);
    }
}
