package ai.grakn.engine.util;

import ai.grakn.exception.GraknEngineServerException;
import com.auth0.jwt.JWTSigner;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.JWTVerifyException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.util.HashMap;
import java.util.Map;

public class JWTHandler {

    static private final String issuer = "https://grakn.ai/";
    static private final String secret = ConfigProperties.getInstance().getProperty(ConfigProperties.JWT_SECRET_PROPERTY);

    static public String signJWT(String username) {
        long iat = System.currentTimeMillis() / 1000L; // issued at claim
        long exp = iat + 3600L; // expires claim. In this case the token expires in 3600 seconds

        final JWTSigner signer = new JWTSigner(secret);
        final Map<String, Object> claims = new HashMap<>();
        claims.put("iss", issuer);
        claims.put("exp", exp);
        claims.put("iat", iat);
        claims.put("username", username);

        return signer.sign(claims);
    }

    static public String extractUserFromJWT(String jwt) {
        try {
            JWTVerifier verifier = new JWTVerifier(secret);
            Map<String, Object> claims = verifier.verify(jwt);
            return claims.get("username").toString();
        } catch (Exception e) {
            throw new GraknEngineServerException(500, e);
        }
    }

    static public boolean verifyJWT(String jwt) {
        try {
            JWTVerifier verifier = new JWTVerifier(secret);
            verifier.verify(jwt);
            return true;
        } catch (JWTVerifyException | NoSuchAlgorithmException | IOException | SignatureException | InvalidKeyException e) {
            return false;
        }
    }
}
