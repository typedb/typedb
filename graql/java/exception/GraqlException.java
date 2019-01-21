package graql.exception;


public class GraqlException extends RuntimeException {

    protected GraqlException(String error) {
        super(error);
    }

    protected GraqlException(String error, Exception e) {
        super(error, e);
    }

    public static GraqlException conflictingProperties(String statement, String property, String other) {
        return new GraqlException(ErrorMessage.CONFLICTING_PROPERTIES.getMessage(statement, property, other));
    }

    public String getName() {
        return this.getClass().getName();
    }
}