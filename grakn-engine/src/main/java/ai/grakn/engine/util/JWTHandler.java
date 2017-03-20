/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.engine.util;

import ai.grakn.engine.GraknEngineConfig;
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

/**
 * <p>
 *     Retrieves and verifies user data using JWT
 * </p>
 *
 * @author Marco Scoppetta
 */
public class JWTHandler {

    static private final String issuer = "https://grakn.ai/";
    static private final String secret = GraknEngineConfig.getInstance().getProperty(GraknEngineConfig.JWT_SECRET_PROPERTY);

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
