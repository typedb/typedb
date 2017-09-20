package ai.grakn.graknmodule.http;

public interface HttpBeforeFilter {
    String getUrlPattern();

    HttpBeforeFilterResult getHttpBeforeFilter(HttpRequest request);
}
