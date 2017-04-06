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
                .relates(geoEntity).relates(entityLocation);

        geographicalObject = graph.putEntityType("geoObject")
                .plays(geoEntity)
                .plays(entityLocation);
        geographicalObject.resource(key);

        continent = graph.putEntityType("continent")
                .superType(geographicalObject)
                .plays(entityLocation);
        country = graph.putEntityType("country")
                .superType(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        region = graph.putEntityType("region")
                .superType(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        city = graph.putEntityType("city")
                .superType(geographicalObject)
                .plays(geoEntity)
                .plays(entityLocation);
        university = graph.putEntityType("university")
                        .plays(geoEntity);
        university.resource(key);
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
                .addRolePlayer(geoEntity, PW)
                .addRolePlayer(entityLocation, Warsaw);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, UW)
                .addRolePlayer(entityLocation, Warsaw);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Imperial)
                .addRolePlayer(entityLocation, London);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, UCL)
                .addRolePlayer(entityLocation, London);

        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Warsaw)
                .addRolePlayer(entityLocation, Masovia);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Masovia)
                .addRolePlayer(entityLocation, Poland);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Wroclaw)
                .addRolePlayer(entityLocation, Silesia);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Silesia)
                .addRolePlayer(entityLocation, Poland);


        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Poland)
                .addRolePlayer(entityLocation, Europe);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, London)
                .addRolePlayer(entityLocation, GreaterLondon);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, GreaterLondon)
                .addRolePlayer(entityLocation, England);

        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, England)
               .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Munich)
                .addRolePlayer(entityLocation, Bavaria);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Bavaria)
                .addRolePlayer(entityLocation, Germany);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Germany)
                .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Milan)
                .addRolePlayer(entityLocation, Lombardy);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Lombardy)
                .addRolePlayer(entityLocation, Italy);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Italy)
                .addRolePlayer(entityLocation, Europe);

        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, Paris)
                .addRolePlayer(entityLocation, IleDeFrance);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, IleDeFrance)
                .addRolePlayer(entityLocation, France);
        isLocatedIn.addRelation()
                .addRolePlayer(geoEntity, France)
                .addRolePlayer(entityLocation, Europe);

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
