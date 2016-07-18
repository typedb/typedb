package io.mindmaps.graql.internal.parser;

/**
 * Includes ANSI unicode commands for different colours
 */
@SuppressWarnings({"unused", "JavaDoc"})
public class ANSI {
    private ANSI() {

    }

    private static final String RESET = "\u001B[0m";
    public static final String BLACK = "\u001B[30m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String PURPLE = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";

    /**
     * @param string the string to set the color on
     * @param color the color to set on the string
     * @return a new string with the color set
     */
    public static String color(String string, String color) {
        return color + string + RESET;
    }
}
