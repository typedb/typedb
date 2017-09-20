package ai.grakn.graknmodule.http;

public interface BeforeHttpEndpoint {
    String getUrlPattern();

    Before getBeforeHttpEndpoint(Request request);
}
