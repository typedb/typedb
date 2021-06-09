/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typedb.core.TypeDB.Session;
import com.vaticle.typedb.core.TypeDB.Transaction;
import com.vaticle.typedb.core.common.parameters.Arguments.Transaction.Type;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.pattern.Conjunction;
import com.vaticle.typeql.lang.pattern.Pattern;

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

            try (Transaction tx = session.transaction(Type.WRITE)) {
                tx.query().match(TypeQL.parseQuery(query).asMatch()).toList();
                tx.commit();
            }
        }
    }

    public static Set<Statement> getStatements(List<Pattern> patternList) {
        LinkedHashSet<Pattern> patternSet = new LinkedHashSet<>(patternList);
        return new Conjunction<>(patternSet).statements();
    }
}
