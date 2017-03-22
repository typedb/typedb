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
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.util.function.Consumer;

import static ai.grakn.graql.Graql.and;

public class GeoGraph extends TestGraph {

    private static ResourceType<String> key;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationType isLocatedIn;

    private static RoleType geoEntity, entityLocation;

    private static Instance Europe, NorthAmerica;
    private static Instance Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Instance Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Instance Poland, England, Germany, France, Italy;
    private static Instance UW, PW, Imperial, UniversityOfMunich, UCL;

    public static Consumer<GraknGraph> get(){
        return new GeoGraph().build();
    }

    @Override
    public void buildOntology(GraknGraph graph) {
        key = graph.putResourceType("name", ResourceType.DataType.STRING);

        geoEntity = graph.putRoleType("geo-entity");
        entityLocation = graph.putRoleType("entity-location");
        isLocatedIn = graph.putRelationType("is-located-in")
                .hasRole(geoEntity).hasRole(entityLocation);

        geographicalObject = graph.putEntityType("geoObject")
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        geographicalObject.hasResource(key);

        continent = graph.putEntityType("continent")
                .superType(geographicalObject)
                .playsRole(entityLocation);
        country = graph.putEntityType("country")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        region = graph.putEntityType("region")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        city = graph.putEntityType("city")
                .superType(geographicalObject)
                .playsRole(geoEntity)
                .playsRole(entityLocation);
        university = graph.putEntityType("university")
                        .playsRole(geoEntity);
        university.hasResource(key);
    }

    @Override
    public void buildInstances(GraknGraph graph) {
        Europe = putEntity(graph, "Europe", continent, key.getName());
        NorthAmerica = putEntity(graph, "NorthAmerica", continent, key.getName());
        Poland = putEntity(graph, "Poland", country, key.getName());
        England = putEntity(graph, "England", country, key.getName());
        Germany = putEntity(graph, "Germany", country, key.getName());
        France = putEntity(graph, "France", country, key.getName());
        Italy = putEntity(graph, "Italy", country, key.getName());
        Masovia = putEntity(graph, "Masovia", region, key.getName());
        Silesia = putEntity(graph, "Silesia", region, key.getName());
        GreaterLondon = putEntity(graph, "GreaterLondon", region, key.getName());
        Bavaria = putEntity(graph, "Bavaria", region, key.getName());
        IleDeFrance = putEntity(graph, "IleDeFrance", region, key.getName());
        Lombardy = putEntity(graph, "Lombardy", region, key.getName());
        Warsaw = putEntity(graph, "Warsaw", city, key.getName());
        Wroclaw = putEntity(graph, "Wroclaw", city, key.getName());
        London = putEntity(graph, "London", city, key.getName());
        Munich = putEntity(graph, "Munich", city, key.getName());
        Paris = putEntity(graph, "Paris", city, key.getName());
        Milan = putEntity(graph, "Milan", city, key.getName());
        UW = putEntity(graph, "University-of-Warsaw", university, key.getName());
        PW = putEntity(graph, "Warsaw-Polytechnics", university, key.getName());
        Imperial = putEntity(graph, "Imperial College London", university, key.getName());
        UCL = putEntity(graph, "University College London", university, key.getName());
        UniversityOfMunich = putEntity(graph, "University of Munich", university, key.getName());
    }

    @Override
    public void buildRelations(GraknGraph graph) {
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, PW)
                .putRolePlayer(entityLocation, Warsaw);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, UW)
                .putRolePlayer(entityLocation, Warsaw);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Imperial)
                .putRolePlayer(entityLocation, London);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, UCL)
                .putRolePlayer(entityLocation, London);

        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Warsaw)
                .putRolePlayer(entityLocation, Masovia);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Masovia)
                .putRolePlayer(entityLocation, Poland);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Wroclaw)
                .putRolePlayer(entityLocation, Silesia);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Silesia)
                .putRolePlayer(entityLocation, Poland);


        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Poland)
                .putRolePlayer(entityLocation, Europe);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, London)
                .putRolePlayer(entityLocation, GreaterLondon);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, GreaterLondon)
                .putRolePlayer(entityLocation, England);

        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, England)
               .putRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Munich)
                .putRolePlayer(entityLocation, Bavaria);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Bavaria)
                .putRolePlayer(entityLocation, Germany);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Germany)
                .putRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Milan)
                .putRolePlayer(entityLocation, Lombardy);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Lombardy)
                .putRolePlayer(entityLocation, Italy);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Italy)
                .putRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, Paris)
                .putRolePlayer(entityLocation, IleDeFrance);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, IleDeFrance)
                .putRolePlayer(entityLocation, France);
        isLocatedIn.addRelation()
                .putRolePlayer(geoEntity, France)
                .putRolePlayer(entityLocation, Europe);

    }

    @Override
    public void buildRules(GraknGraph graph) {
        RuleType inferenceRule = graph.admin().getMetaRuleInference();
        Pattern transitivity_LHS = and(graph.graql().parsePatterns(
                "(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;"));
        Pattern transitivity_RHS = and(graph.graql().parsePatterns("(geo-entity: $x, entity-location: $z) isa is-located-in;"));
        inferenceRule.putRule(transitivity_LHS, transitivity_RHS);
    }
}
