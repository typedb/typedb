package ai.grakn.util;

/**
 * Class for storing the maven version. The templating-maven-plugin in grakn-dist will automatically insert the
 * project version here.
 */
public class GraknVersion {
    public static final String VERSION = "${project.version}";
}
