/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.reasoner.graph;

import grakn.core.graql.Query;
import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.Thing;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

@SuppressWarnings("CheckReturnValue")
public abstract class ParametrisedTestGraph {
    private final Label key;
    private final static String gqlPath = "test-integration/graql/reasoner/resources/";
    private final String schemaFile;
    private final Session session;

    ParametrisedTestGraph(Session session, String schemaFile, Label key){
        this.session = session;
        this.schemaFile = schemaFile;
        this.key = key;
    }

    private void loadSchema(Session session) {
        try {
            System.out.println("Loading... " + gqlPath + schemaFile);
            InputStream inputStream = ParametrisedTestGraph.class.getClassLoader().getResourceAsStream(gqlPath + schemaFile);
            String s = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
            Transaction tx = session.transaction(Transaction.Type.WRITE);
            tx.graql().parser().parseList(s).forEach(Query::execute);
            tx.commit();
        } catch (Exception e) {
            System.err.println(e);
            throw new RuntimeException(e);
        }
    }

    public final void load(int n) {
        loadSchema(session);
        buildExtensionalDB(n);
    }

    public final void load(int n, int m) {
        loadSchema(session);
        buildExtensionalDB(n, m);
    }

    Label key(){ return key;}

    Transaction tx(){ return session.transaction(Transaction.Type.WRITE); }

    abstract protected void buildExtensionalDB(int n, int children);
    abstract protected void buildExtensionalDB(int n);

    static Thing putEntityWithResource(Transaction tx, String id, EntityType type, Label key) {
        Thing inst = type.create();
        putResource(inst, tx.getSchemaConcept(key), id);
        return inst;
    }

    static Thing getInstance(Transaction tx, String id){
        Set<Thing> things = tx.getAttributesByValue(id)
                .stream().flatMap(Attribute::owners).collect(toSet());
        if (things.size() != 1) {
            throw new IllegalStateException("Multiple things with given resource value");
        }
        return things.iterator().next();
    }

    private static <T> void putResource(Thing thing, AttributeType<T> attributeType, T resource) {
        Attribute attributeInstance = attributeType.create(resource);
        thing.has(attributeInstance);
    }
}
