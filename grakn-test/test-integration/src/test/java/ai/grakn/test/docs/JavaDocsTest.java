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

package ai.grakn.test.docs;

import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.GraknTestUtil;
import groovy.util.Eval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ai.grakn.test.docs.DocTestUtil.PAGES;
import static ai.grakn.test.docs.DocTestUtil.allMarkdownFiles;
import static ai.grakn.test.docs.DocTestUtil.codeBlockFail;
import static ai.grakn.test.docs.DocTestUtil.markdownOrHtml;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for the Java/Groovy examples in the documentation.
 *
 * <p>
 *     Will automatically find and execute all code examples marked {@code java} in the documentation. If you don't
 *     want an example to run in the tests, annotate it with something other than {@code java}, such as
 *     {@code java-skip-test}.
 * </p>
 *
 * <p>
 *     Each example is run using a Groovy interpreter, this means that the examples should strictly be valid Groovy, not
 *     Java. Groovy is chosen because it can be interpreted at runtime. Groovy is nearly a super-set of Java, so this
 *     is usually not a problem. One example where syntax differs is Java lambdas.
 * </p>
 *
 * <p>
 *     Each example is run using a pre-loaded graph. Go to {@link DocTestUtil#loaders} for more information.
 * </p>
 */

@RunWith(Parameterized.class)
public class JavaDocsTest {

    private static final Pattern TAG_JAVA = markdownOrHtml("java");

    private static String groovyPrefix;

    @Parameterized.Parameter(0)
    public File file;

    @Parameterized.Parameter(1)
    public Path name;

    private static int numFound = 0;

    @ClassRule
    public static EngineContext engine = EngineContext.create();

    public static String knowledgeBaseName;

    @Parameterized.Parameters(name = "{1}")
    public static Collection files() {
        return allMarkdownFiles().stream()
                .map(file -> new Object[] {file, PAGES.toPath().relativize(file.toPath())})
                .collect(toList());
    }

    @BeforeClass
    public static void loadGroovyPrefix() throws IOException {
        groovyPrefix = new String(Files.readAllBytes(Paths.get("src/test/java/ai/grakn/test/docs/prefix.groovy")));
    }

    @AfterClass
    public static void assertEnoughExamplesFound() {
        if (numFound < 8) {
            fail("Only found " + numFound + " Java examples. Perhaps the regex is wrong?");
        }
    }

    @Test
    public void testExamplesValidGroovy() throws IOException {
        byte[] encoded = Files.readAllBytes(file.toPath());

        String contents = new String(encoded, StandardCharsets.UTF_8);

        knowledgeBaseName = DocTestUtil.getKnowledgeBaseName(contents);

        Matcher matcher = TAG_JAVA.matcher(contents);

        String groovyString = "";
        boolean foundGroovy = false;

        while (matcher.find()) {
            String match = matcher.group(2);

            String trimmed = match.trim();

            if (!(trimmed.startsWith("-test-ignore") || trimmed.startsWith("<!--test-ignore-->"))) {
                numFound += 1;
                groovyString += matcher.group(2) + "\n";
                foundGroovy = true;
            }
        }

        String indented = DocTestUtil.indent(groovyString);
        groovyString = groovyPrefix.replaceFirst("\\$putTheBodyHere", Matcher.quoteReplacement(indented));

        if (foundGroovy) {
            String fileAndLine = file.getName() + ":1";
            assertGroovyStringValid(fileAndLine, groovyString);
        }
    }

    private void assertGroovyStringValid(String fileAndLine, String groovyString) {
        try {
            Eval.me(groovyString.replaceAll("\\$", "\\\\\\$"));
        } catch (Exception e) {
            e.printStackTrace();
            codeBlockFail(fileAndLine, groovyString, e.getMessage());
        }
    }
}
