package ai.grakn.engine.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import static java.util.stream.Collectors.toList;

public class ConcurrencyUtil {
    static public <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> cf) {
        return CompletableFuture.allOf(cf.toArray(new CompletableFuture[cf.size()]))
                .thenApply(v -> cf.stream()
                        .map(CompletableFuture::join)
                        .collect(toList())
                );
    }
}
