/*
 *  GRAKN.AI - THE KNOWLEDGE GRAPH
 *  Copyright (C) 2018 Grakn Labs Ltd
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package storage;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Role;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.util.Schema;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.pattern.Patterns.var;

/**
 *
 */
public class SchemaManager {

    public static void initialise(GraknSession session, String schemaRelativeDirPath) {
        /*
        Delete all concepts and concept types in Grakn, and load a schema from file
         */
        File graql = new File(System.getProperty("user.dir") + schemaRelativeDirPath);

        List<String> queries;
        try {
            queries = Files.readLines(graql, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (GraknTx tx = session.transaction(GraknTxType.WRITE)) {

            QueryBuilder qb = tx.graql();
            Var x = Graql.var().asUserDefined();  //TODO This needed to be asUserDefined or else getting error: ai.grakn.exception.GraqlQueryException: the variable $1528883020589004 is not in the query
            Var y = Graql.var().asUserDefined();

            // qb.match(x.isa("thing")).delete(x).execute();  // TODO Only got a complaint at runtime when using delete() without supplying a variable
            // TODO Sporadically has errors, logged in bug #20200

            qb.match(x.isa("attribute")).delete(x).execute();
            qb.match(x.isa("relationship")).delete(x).execute();
            qb.match(x.isa("entity")).delete(x).execute();

            //
//            qb.undefine(y.sub("thing")).execute(); // TODO undefine $y sub thing; doesn't work/isn't supported
            // TODO undefine $y sub entity; also doesn't work, you need to be specific with undefine

            List<ConceptMap> schema = qb.match(y.sub("thing")).get().execute();

            for (ConceptMap element : schema) {
                Var z = Graql.var().asUserDefined();
                qb.undefine(z.id(element.get(y).id())).execute();
            }

            tx.graql().parser().parseList(queries.stream().collect(Collectors.joining("\n"))).forEach(Query::execute);
            tx.commit();
        }
    }

    public static <T extends Type> HashSet<T> getTypesOfMetaType(GraknTx tx, String metaTypeName) {
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(metaTypeName));
        List<ConceptMap> result = match.get().execute();

        return result.stream()
                .map(answer -> (T) answer.get(var("x")).asType())
                .filter(type -> !type.isImplicit())
                .filter(type -> !Schema.MetaSchema.isMetaLabel(type.label()))
                .collect(Collectors.toCollection(HashSet::new));
    }

    public static HashSet<Role> getRoles(GraknTx tx, String metaTypeName) {
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(metaTypeName));
        List<ConceptMap> result = match.get().execute();

        return result.stream()
                .map(answer -> answer.get(var("x")).asRole())
                .filter(type -> !type.isImplicit())
                .filter(type -> !Schema.MetaSchema.isMetaLabel(type.label()))
                .collect(Collectors.toCollection(HashSet::new));
    }

    public static <T extends SchemaConcept> T getTypeFromString(String typeName, HashSet<T> typeInstances) {
        Iterator iter = typeInstances.iterator();
        String l;
        T currentType;

        while (iter.hasNext()) {
            currentType = (T) iter.next();
            l = currentType.label().toString();
            if (l.equals(typeName)) {
                return currentType;
            }
        }
        throw new RuntimeException("Couldn't find a concept type with name \"" + typeName + "\"");
    }

    public static AttributeType.DataType getDatatype(String typeName, HashSet<Type> typeInstances) {
        return null;
    }
}
