/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.kbs;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Pattern;
import ai.grakn.test.rule.SampleKBContext;

/**
 *
 * @author Kasper Piskorski
 *
 */
public class GeoKB extends TestKB {

    private static AttributeType<String> key;

    private static EntityType university, city, region, country, continent, geographicalObject;
    private static RelationshipType isLocatedIn;

    private static Role geoEntity, entityLocation;

    private static Thing Europe;
    private static Thing Warsaw, Wroclaw, London, Munich, Paris, Milan;
    private static Thing Masovia, Silesia, GreaterLondon, Bavaria, IleDeFrance, Lombardy;
    private static Thing Poland, England, Germany, France, Italy;
    private static Thing UW;
    private static Thing PW;
    private static Thing Imperial;
    private static Thing UCL;

    public static SampleKBContext context(){
        return new GeoKB().makeContext();
    }

    @Override
    public void buildSchema(GraknTx tx) {
        key = tx.putAttributeType("name", AttributeType.DataType.STRING);

        geoEntity = tx.putRole("geo-entity");
        entityLocation = tx.putRole("entity-location");
        isLocatedIn = tx.putRelationshipType("is-located-in")
                .relate(geoEntity).relate(entityLocation);

        geographicalObject = tx.putEntityType("geoObject")
                .play(geoEntity)
                .play(entityLocation);
        geographicalObject.has(key);

        continent = tx.putEntityType("continent")
                .sup(geographicalObject)
                .play(entityLocation);
        country = tx.putEntityType("country")
                .sup(geographicalObject)
                .play(geoEntity)
                .play(entityLocation);
        region = tx.putEntityType("region")
                .sup(geographicalObject)
                .play(geoEntity)
                .play(entityLocation);
        city = tx.putEntityType("city")
                .sup(geographicalObject)
                .play(geoEntity)
                .play(entityLocation);
        university = tx.putEntityType("university")
                        .play(geoEntity);
        university.has(key);
    }

    @Override
    public void buildInstances(GraknTx tx) {
        Europe = putEntityWithResource(tx, "Europe", continent, key.label());

        Poland = putEntityWithResource(tx, "Poland", country, key.label());
        Masovia = putEntityWithResource(tx, "Masovia", region, key.label());
        Silesia = putEntityWithResource(tx, "Silesia", region, key.label());
        Warsaw = putEntityWithResource(tx, "Warsaw", city, key.label());
        Wroclaw = putEntityWithResource(tx, "Wroclaw", city, key.label());
        UW = putEntityWithResource(tx, "University-of-Warsaw", university, key.label());
        PW = putEntityWithResource(tx, "Warsaw-Polytechnics", university, key.label());

        England = putEntityWithResource(tx, "England", country, key.label());
        GreaterLondon = putEntityWithResource(tx, "GreaterLondon", region, key.label());
        London = putEntityWithResource(tx, "London", city, key.label());
        Imperial = putEntityWithResource(tx, "Imperial College London", university, key.label());
        UCL = putEntityWithResource(tx, "University College London", university, key.label());

        Germany = putEntityWithResource(tx, "Germany", country, key.label());
        Bavaria = putEntityWithResource(tx, "Bavaria", region, key.label());
        Munich = putEntityWithResource(tx, "Munich", city, key.label());
        putEntityWithResource(tx, "University of Munich", university, key.label());

        France = putEntityWithResource(tx, "France", country, key.label());
        IleDeFrance = putEntityWithResource(tx, "IleDeFrance", region, key.label());
        Paris = putEntityWithResource(tx, "Paris", city, key.label());

        Italy = putEntityWithResource(tx, "Italy", country, key.label());
        Lombardy = putEntityWithResource(tx, "Lombardy", region, key.label());
        Milan = putEntityWithResource(tx, "Milan", city, key.label());
    }

    @Override
    public void buildRelations() {

        isLocatedIn.create()
                .assign(geoEntity, Poland)
                .assign(entityLocation, Europe);

        isLocatedIn.create()
                .assign(geoEntity, Masovia)
                .assign(entityLocation, Poland);

        isLocatedIn.create()
                .assign(geoEntity, Warsaw)
                .assign(entityLocation, Masovia);

        isLocatedIn.create()
                .assign(geoEntity, PW)
                .assign(entityLocation, Warsaw);

        isLocatedIn.create()
                .assign(geoEntity, UW)
                .assign(entityLocation, Warsaw);

        isLocatedIn.create()
                .assign(geoEntity, Silesia)
                .assign(entityLocation, Poland);

        isLocatedIn.create()
                .assign(geoEntity, Wroclaw)
                .assign(entityLocation, Silesia);



        isLocatedIn.create()
                .assign(geoEntity, Imperial)
                .assign(entityLocation, London);
        isLocatedIn.create()
                .assign(geoEntity, UCL)
                .assign(entityLocation, London);
        isLocatedIn.create()
                .assign(geoEntity, London)
                .assign(entityLocation, GreaterLondon);
        isLocatedIn.create()
                .assign(geoEntity, GreaterLondon)
                .assign(entityLocation, England);
        isLocatedIn.create()
                .assign(geoEntity, England)
               .assign(entityLocation, Europe);

        isLocatedIn.create()
                .assign(geoEntity, Munich)
                .assign(entityLocation, Bavaria);
        isLocatedIn.create()
                .assign(geoEntity, Bavaria)
                .assign(entityLocation, Germany);
        isLocatedIn.create()
                .assign(geoEntity, Germany)
                .assign(entityLocation, Europe);

        isLocatedIn.create()
                .assign(geoEntity, Milan)
                .assign(entityLocation, Lombardy);
        isLocatedIn.create()
                .assign(geoEntity, Lombardy)
                .assign(entityLocation, Italy);
        isLocatedIn.create()
                .assign(geoEntity, Italy)
                .assign(entityLocation, Europe);

        isLocatedIn.create()
                .assign(geoEntity, Paris)
                .assign(entityLocation, IleDeFrance);
        isLocatedIn.create()
                .assign(geoEntity, IleDeFrance)
                .assign(entityLocation, France);
        isLocatedIn.create()
                .assign(geoEntity, France)
                .assign(entityLocation, Europe);
    }

    @Override
    public void buildRules(GraknTx tx) {
        Pattern transitivity_LHS = tx.graql().parser().parsePattern("{(geo-entity: $x, entity-location: $y) isa is-located-in;" +
                "(geo-entity: $y, entity-location: $z) isa is-located-in;}");
        Pattern transitivity_RHS = tx.graql().parser().parsePattern("{(geo-entity: $x, entity-location: $z) isa is-located-in;}");
        tx.putRule("Geo Rule", transitivity_LHS, transitivity_RHS);
    }
}
