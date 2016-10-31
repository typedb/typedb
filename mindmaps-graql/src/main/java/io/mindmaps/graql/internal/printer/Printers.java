package io.mindmaps.graql.internal.printer;

import io.mindmaps.graql.Printer;

public class Printers {

    private Printers() {}

    public static Printer graql() {
        return new GraqlPrinter();
    }
}
