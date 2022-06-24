/*
 * Copyright (C) 2022 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.common.test;

import com.vaticle.typedb.core.common.exception.TypeDBException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Util {

    public static void assertThrowsTypeDBException(Runnable function, String errorCode) {
        try {
            function.run();
            fail();
        } catch (TypeDBException e) {
            assert e.code().isPresent();
            assertEquals(errorCode, e.code().get());
        } catch (Exception e) {
            fail();
        }
    }

    public static void assertThrows(Runnable function) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assertTrue(true);
        }
    }

    public static <T extends RuntimeException> void assertThrows(Runnable function, Class<T> exceptionType) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assertTrue(exceptionType.isInstance(e));
        }
    }

    public static void assertNotThrows(Runnable function) {
        try {
            function.run();
        } catch (Exception e) {
            // fail but we want to see the exception
            throw e;
        }
    }

    public static void assertThrowsWithMessage(Runnable function, String message) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assert (e.getMessage().contains(message));
        }
    }

}
