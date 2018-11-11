/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.parser;

import com.google.auto.value.AutoValue;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenFactory;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.UnbufferedTokenStream;

/**
 * Implementation of {@link TokenSource} that wraps another {@link TokenSource}, only allowing tokens through if they
 * match the expected channel.
 * <p>
 *     This class is necessary when using the {@link UnbufferedTokenStream} to parse extremely large files, because
 *     that class does not filter out e.g. whitespace and comments.
 * </p>
 *
 */
@AutoValue
public abstract class ChannelTokenSource implements TokenSource {

    abstract TokenSource source();
    abstract int channel();

    public static ChannelTokenSource of(TokenSource source) {
        return of(source, Token.DEFAULT_CHANNEL);
    }

    public static ChannelTokenSource of(TokenSource source, int channel) {
        return new AutoValue_ChannelTokenSource(source, channel);
    }

    @Override
    public Token nextToken() {
        Token token;

        do {
            token = source().nextToken();
        } while (token.getChannel() != channel());

        return token;
    }

    @Override
    public int getLine() {
        return source().getLine();
    }

    @Override
    public int getCharPositionInLine() {
        return source().getCharPositionInLine();
    }

    @Override
    public CharStream getInputStream() {
        return source().getInputStream();
    }

    @Override
    public String getSourceName() {
        return source().getSourceName();
    }

    @Override
    public void setTokenFactory(TokenFactory<?> factory) {
        source().setTokenFactory(factory);
    }

    @Override
    public TokenFactory<?> getTokenFactory() {
        return source().getTokenFactory();
    }
}
