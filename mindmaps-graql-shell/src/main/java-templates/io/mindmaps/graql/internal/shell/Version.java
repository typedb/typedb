package io.mindmaps.graql.internal.shell;

/**
 * Class for storing the maven version. The templating-maven-plugin in mindmaps-dist will automatically insert the
 * project version here.
 */
public class Version {
    public static final String VERSION = "${project.version}";
}
