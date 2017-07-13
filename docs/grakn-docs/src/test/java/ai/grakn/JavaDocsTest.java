package ai.grakn;

import groovy.util.Eval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ai.grakn.DocTestUtil.PAGES;
import static ai.grakn.DocTestUtil.allMarkdownFiles;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class JavaDocsTest {

    private static final Pattern TAG_JAVA =
            Pattern.compile(
                    "(id=\"java[0-9]+\">\\s*<pre>|```java)" +
                    "\\s*(.*?)\\s*" +
                    "(</pre>|```)", Pattern.DOTALL);

    private static String groovyPrefix;
    private final File file;
    private static int numFound = 0;

    public JavaDocsTest(File file, @SuppressWarnings("UnusedParameters") String name) {
        this.file = file;
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection files() {
        return allMarkdownFiles().stream()
                .map(file -> new Object[] {file, file.toString().substring(PAGES.length())})
                .collect(toList());
    }

    @BeforeClass
    public static void loadGroovyPrefix() throws IOException {
        groovyPrefix = new String(Files.readAllBytes(Paths.get("src/test/java/ai/grakn/prefix.groovy")));
    }

    @AfterClass
    public static void assertEnoughExamplesFound() {
        if (numFound < 10) {
            fail("Only found " + numFound + " Java examples. Perhaps the regex is wrong?");
        }
    }

    @Test
    public void testExamplesValidGroovy() throws IOException {
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            fail();
        }

        String contents = new String(encoded, StandardCharsets.UTF_8);

        Matcher matcher = TAG_JAVA.matcher(contents);

        String groovyString = groovyPrefix;

        while (matcher.find()) {
            String match = matcher.group(2);
            if (!match.trim().startsWith("-test-ignore")) {
                numFound += 1;
                groovyString += matcher.group(2) + "\n";
            }
        }

        assertGroovyStringValid(file.toString(), groovyString);
    }

    private void assertGroovyStringValid(String fileName, String groovyString) {
        try {
            Eval.me(groovyString.replaceAll("\\$", "\\\\\\$"));
        } catch (Exception e) {
            e.printStackTrace();
            compilationFail(fileName, groovyString, e.getMessage());
        }
    }

    private void compilationFail(String fileName, String groovyString, String error) {
        fail("Invalid Groovy in " + fileName + ":\n" + groovyString + "\nERROR:\n" + error);
    }
}
