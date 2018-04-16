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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.test.docs;

import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.exception.GraknException;
import ai.grakn.exception.GraqlSyntaxException;
import ai.grakn.graql.Query;
import ai.grakn.test.rule.EngineContext;
import ai.grakn.util.GraknTestUtil;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ai.grakn.test.docs.DocTestUtil.PAGES;
import static ai.grakn.test.docs.DocTestUtil.allMarkdownFiles;
import static ai.grakn.test.docs.DocTestUtil.getLineNumber;
import static ai.grakn.test.docs.DocTestUtil.markdownOrHtml;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for the Graql examples in the documentation.
 *
 * <p>
 *     Will automatically find and execute all code examples marked {@code graql} in the documentation. If you don't
 *     want an example to run in the tests, annotate it with something other than {@code graql}, such as
 *     {@code graql-skip-test}.
 * </p>
 *
 * <p>
 *     Each example is run using a pre-loaded graph. Go to {@link DocTestUtil#loaders} for more information.
 * </p>
 */
@RunWith(Parameterized.class)
public class GraqlDocsTest {

    private static final Pattern TAG_GRAQL = markdownOrHtml("graql");
    private static final Pattern TEMPLATE_GRAQL = markdownOrHtml("graql-template");

    private static final Pattern SHELL_GRAQL = Pattern.compile("^*>>>(.*?)$", Pattern.MULTILINE);

    @Parameterized.Parameter(0)
    public File file;

    @Parameterized.Parameter(1)
    public Path name;

    private static final Pattern GRAQL_COMMIT = Pattern.compile("^(.*?)(\\scommit;?)?$", Pattern.DOTALL);

    private static int numFound = 0;

    @ClassRule
    public static EngineContext engine = EngineContext.create();

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> files() {
        return allMarkdownFiles().stream()
                .map(file -> new Object[] {file, PAGES.toPath().relativize(file.toPath())})
                .collect(toList());
    }

    @AfterClass
    public static void assertEnoughExamplesFound() {
        if (GraknTestUtil.usingTinker() && numFound < 10) {
            fail("Only found " + numFound + " Graql examples. Perhaps the regex is wrong?");
        }
    }

    @BeforeClass
    public static void onlyRunOnTinker(){
        assumeTrue(GraknTestUtil.usingTinker());
    }

    @Test
    public void testExamplesValidSyntax() throws IOException {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            fail();
        }

        String contents = new String(encoded, StandardCharsets.UTF_8);

        String knowledgeBaseName = DocTestUtil.getKnowledgeBaseName(contents);

        try (GraknTx graph = DocTestUtil.getTestGraph(engine.uri(), knowledgeBaseName).open(GraknTxType.WRITE)) {
            executeAssertionOnContents(graph, TAG_GRAQL, file, contents, this::assertGraqlCodeblockValidSyntax);

            // TODO: Fix issue with this test when template expects data in a certain format
//            executeAssertionOnContents(graph, TEMPLATE_GRAQL, file, contents, this::assertGraqlTemplateValidSyntax);
        }
    }

    private void executeAssertionOnContents(GraknTx graph, Pattern pattern, File file, String contents,
                                            TriConsumer<GraknTx, String, String> assertion) throws IOException {
        Matcher matcher = pattern.matcher(contents);

        while (matcher.find()) {
            numFound += 1;

            String graqlString = matcher.group("query");

            System.out.println(graqlString);

            String trimmed = graqlString.trim();

            if (!(trimmed.startsWith("test-ignore") || trimmed.startsWith("<!--test-ignore-->"))) {
                String fileAndLine = file.getName() + ":" + getLineNumber(contents, matcher.toMatchResult().start());

                assertion.accept(graph, fileAndLine, graqlString);
            }
        }
    }

    private void assertGraqlCodeblockValidSyntax(GraknTx graph, String fileAndLine, String block) {
        Matcher shellMatcher = SHELL_GRAQL.matcher(block);

        if (shellMatcher.find()) {
            while (shellMatcher.find()) {
                String graqlString = shellMatcher.group(1);
                assertGraqlStringValidSyntax(graph, fileAndLine, graqlString);
            }
        } else {
            assertGraqlStringValidSyntax(graph, fileAndLine, block);
        }
    }

    private void assertGraqlStringValidSyntax(GraknTx graph, String fileAndLine, String graqlString) {
        try {
            parse(graph, graqlString);
        } catch (GraknException e1) {
            DocTestUtil.codeBlockFail(fileAndLine, graqlString, e1.getMessage());
        }
    }

    private void assertGraqlTemplateValidSyntax(GraknTx graph, String fileName, String templateBlock){
        // An endless map where you always end up back where you started like in Zelda
        Map<String, Object> data = Mockito.mock(Map.class);
        when(data.containsKey(any())).thenReturn(true);
        when(data.get(any())).thenReturn(data);
        when(data.toString()).thenReturn("\"MOCK\"");

        try {
            graph.graql().parser().parseTemplate(templateBlock, data);
        } catch (GraqlSyntaxException e){
            DocTestUtil.codeBlockFail(fileName, templateBlock, e.getMessage());
        } catch (Exception e){}
    }

    private void parse(GraknTx graph, String line) {
        // TODO: should `commit` be considered valid Graql? It strictly isn't.
        Matcher matcher = GRAQL_COMMIT.matcher(line);
        matcher.find();
        line = matcher.group(1);
        graph.graql().parser().parseList(line).forEach(Query::execute);
    }

}
