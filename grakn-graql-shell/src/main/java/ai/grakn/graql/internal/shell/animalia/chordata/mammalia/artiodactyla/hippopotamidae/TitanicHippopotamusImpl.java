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
 *
 */

package ai.grakn.graql.internal.shell.animalia.chordata.mammalia.artiodactyla.hippopotamidae;

/**
 * A huge hippo. Its hunger is immeasurable. Master of disguise.
 *
 * @author Felix Chapman
 */
public class TitanicHippopotamusImpl implements Hippopotamus {

    private String me =
            "                     .^.,*.\n" +
            "                    (   )  )\n" +
            "                   .~       \"-._   _.-'-*'-*'-*'-*'-'-.--._\n" +
            "                 /'             `\"'                        `.\n" +
            "               _/'                                           `.\n" +
            "          __,\"\"                                                ).--.\n" +
            "       .-'       `._.'                                          .--.\\\n" +
            "      '                                                         )   \\`:\n" +
            "     ;                                                          ;    \"\n" +
            "    :                                                           )\n" +
            "    | 8                                                        ;\n" +
            "     =                  )                                     .\n" +
            "      \\                .                                    .'\n" +
            "       `.            ~  \\                                .-'\n" +
            "         `-._ _ _ . '    `.          ._        _        |\n" +
            "                           |        /  `\"-*--*' |       |  mb\n" +
            "                           |        |           |       :\n";

    @Override
    public void submerge() {
        me =
                "                     .^.,*.\n" +
                "                    (   )  )\n" +
                "                   .~       \"-._   _.-'-*'-*'-*'-*'-'-.--._\n" +
                "                 /'             `\"'                        `.\n" +
                "               _/'                                           `.\n" +
                "          __,\"\"                                                ).--.\n" +
                "       .-'       `._.'                                          .--.\\\n" +
                "      '                                                         )   \\`:\n" +
                "     ;                                                          ;    \"\n" +
                "    :                                                           )\n" +
                "    | 8                                                        ;\n" +
                "     =                  )                                     .\n" +
                "      \\                .                                    .'\n" +
                "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ > ";
    }

    @Override
    public boolean isHungryHungry() {
        throw new StackOverflowError();
    }

    @Override
    public String toString() {
        return me;
    }
}
