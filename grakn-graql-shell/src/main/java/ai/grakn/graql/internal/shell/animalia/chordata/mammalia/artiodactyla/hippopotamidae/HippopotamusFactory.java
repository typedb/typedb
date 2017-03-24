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

import jline.console.ConsoleReader;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;

/**
 * A factory for building a {@link Hippopotamus}, providing an integer size. The resulting hippopotamus will vary based
 * on the size provided. Namely, the size will impact the representation of the hippo and also whether the hippo is
 * hungry hungry. A hungry hungry hippo does not behave differently to a normal hippo, but can be an indication of its
 * size and ferocity.
 *
 * @see Hippopotamus#isHungryHungry()
 *
 * @author Felix Chapman
 */
public class HippopotamusFactory {

    private int size = 10;

    public static HippopotamusFactory builder() {
        return new HippopotamusFactory();
    }

    public HippopotamusFactory size(int size) {
        this.size = size;
        return this;
    }

    public Hippopotamus build() {
        if (size < 5) {
            return new SmallHippopotamusImpl();
        } else if (size < Integer.MAX_VALUE) {
            return new LargeHippopotamusImpl();
        } else {
            return new TitanicHippopotamusImpl();
        }
    }

    /**
     * Raise population of hippopotamus amphibius within console.
     * @param console
     */
    public static void increasePop(ConsoleReader console) {
        HippopotamusFactory builder = HippopotamusFactory.builder();

        if (System.getenv("HIPPO_SIZE") != null) {
            int hippoSize = Integer.parseInt(System.getenv("HIPPO_SIZE"));
            builder.size(hippoSize);
        }

        Hippopotamus hippo = builder.build();

        try {
            for (double i = 0; i < Math.PI; i += 0.1) {
                console.println(hippo.toString().replaceAll("^|\n", "\n" + StringUtils.repeat(" ", (int) (Math.sin(i) * 100))));
                console.flush();
                Thread.sleep(100);
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Supercalafrajilistichippopotamusexception");
        }

        hippo.submerge();

        console.setPrompt(hippo.toString());
    }
}
