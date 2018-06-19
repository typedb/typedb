package storage;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.graql.*;
import ai.grakn.graql.admin.Answer;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static ai.grakn.graql.internal.pattern.Patterns.var;

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

        try (GraknTx tx = session.open(GraknTxType.WRITE)) {

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

            List<Answer> schema = qb.match(y.sub("thing")).get().execute();

            for (Answer element : schema) {
                Var z = Graql.var().asUserDefined();
                qb.undefine(z.id(element.get(y).getId())).execute();
            }

            tx.graql().parser().parseList(queries.stream().collect(Collectors.joining("\n"))).forEach(Query::execute);
            tx.commit();
        }
    }

    public static <T extends SchemaConcept> ArrayList<T> getTypes(GraknTx tx, String conceptTypeName) {
        ArrayList<T> conceptTypes = new ArrayList<T>();
        QueryBuilder qb = tx.graql();
        Match match = qb.match(var("x").sub(conceptTypeName));
        List<Answer> result = match.get().execute();
        T conceptType;

        // TODO This instead?
        // this.conceptTypes.add(result.iterator().forEachRemaining(get("x").asEntityType()));
        Iterator<Answer> conceptTypeIterator = result.iterator();
        while (conceptTypeIterator.hasNext()) {
//            conceptType = (T) conceptTypeIterator.next().get("x").asType();
            conceptType = (T) conceptTypeIterator.next().get("x");
            conceptTypes.add(conceptType);
        }
        conceptTypes.remove(0);  // Remove type "entity"

        return conceptTypes;
    }

    public static <T extends SchemaConcept> T getTypeFromString(String typeName, ArrayList<T> typeInstances) {
        Iterator iter = typeInstances.iterator();
        String l;
        T currentType;

        while (iter.hasNext()) {
            currentType = (T) iter.next();
            l = currentType.getLabel().toString();
            if (l.equals(typeName)) {
                return currentType;
            }
        }
        throw new RuntimeException("Couldn't find a concept type with name \"" + typeName + "\"");
    }
}
