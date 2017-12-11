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

package ai.grakn.dist.lock;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author Ganeshwara Herawan Hananda
 */

public class MkdirLock implements Lock {

    private final String pathToLockDir;

    public MkdirLock(String pathToLockDir) {
        this.pathToLockDir = pathToLockDir;
    }
    @Override
    public void withLock(Runnable fn) {
        try {
            Files.createDirectory(Paths.get(pathToLockDir));
            fn.run();
            Files.delete(Paths.get(pathToLockDir));
        }
        catch (FileAlreadyExistsException ex) {
            throw new LockAlreadyAcquiredException();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
