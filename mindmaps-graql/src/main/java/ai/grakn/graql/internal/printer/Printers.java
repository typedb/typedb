package ai.grakn.graql.internal.printer;

import ai.grakn.graql.Printer;
import ai.grakn.graql.Printer;

public class Printers {

    private Printers() {}

    public static Printer graql() {
        return new GraqlPrinter();
    }

    public static Printer json() {
        return new JsonPrinter();
    }
}
