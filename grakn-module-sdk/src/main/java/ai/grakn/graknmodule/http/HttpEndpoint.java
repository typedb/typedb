package ai.grakn.graknmodule.http;

public interface HttpEndpoint {
    HttpMethods.HTTP_METHOD getHttpMethod();
    String getEndpoint();

    Response getRequestHandler(Request request);
}
