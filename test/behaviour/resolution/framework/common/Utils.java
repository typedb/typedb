package grakn.core.test.behaviour.resolution.framework.common;

import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlQuery;
import graql.lang.statement.Statement;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Utils {

    public static void loadGqlFile(Session session, Path... gqlPath) throws IOException {
        for (Path path : gqlPath) {
            String query = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);

            try (Transaction tx = session.transaction(Transaction.Type.WRITE)) {
                tx.execute((GraqlQuery) Graql.parse(query));
                tx.commit();
            }
        }
    }

    public static Set<Statement> getStatements(List<Pattern> patternList) {
        LinkedHashSet<Pattern> patternSet = new LinkedHashSet<>(patternList);
        return new Conjunction<>(patternSet).statements();
    }
}
