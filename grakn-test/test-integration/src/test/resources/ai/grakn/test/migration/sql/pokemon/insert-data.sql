---
-- #%L
-- test-integration
-- %%
-- Copyright (C) 2016 - 2018 Grakn Labs Ltd
-- %%
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.
-- #L%
---
INSERT INTO pokemon VALUES (1,'bulbasaur' ,1 ,7,69,64,1,1,12,4);
INSERT INTO pokemon VALUES (2,'ivysaur'   ,2 ,10,130,142,2,1,12,4);
INSERT INTO pokemon VALUES (3,'venusaur'  ,3 ,20,1000,236,3,1,12,4);
INSERT INTO pokemon VALUES (4,'charmander',4 ,6,85,62,5,1,10,NULL);
INSERT INTO pokemon VALUES (5,'charmeleon',5 ,11,190,142,6,1,10,NULL);
INSERT INTO pokemon VALUES (6,'charizard' ,6 ,17,905,240,7,1,10,3);
INSERT INTO pokemon VALUES (7,'squirtle'  ,7 ,5,90,63,10,1,11,NULL);
INSERT INTO pokemon VALUES (8,'wartortle' ,8 ,10,225,142,11,1,11,NULL);
INSERT INTO pokemon VALUES (9,'blastoise' ,9 ,16,855,239,12,1,11,NULL);

INSERT INTO type VALUES(1,'normal',1,2);
INSERT INTO type VALUES(2,'fighting',1,2);
INSERT INTO type VALUES(3,'flying',1,2);
INSERT INTO type VALUES(4,'poison',1,2);
INSERT INTO type VALUES(5,'ground',1,2);
INSERT INTO type VALUES(6,'rock',1,2);
INSERT INTO type VALUES(7,'bug',1,2);
INSERT INTO type VALUES(8,'ghost',1,2);
INSERT INTO type VALUES(9,'steel',2,2);
INSERT INTO type VALUES(10,'fire',1,3);
INSERT INTO type VALUES(11,'water',1,3);
INSERT INTO type VALUES(12,'grass',1,3);
INSERT INTO type VALUES(13,'electric',1,3);
INSERT INTO type VALUES(14,'psychic',1,3);
INSERT INTO type VALUES(15,'ice',1,3);
INSERT INTO type VALUES(16,'dragon',1,3);
INSERT INTO type VALUES(17,'dark',2,3);
INSERT INTO type VALUES(18,'fairy',6,NULL);
INSERT INTO type VALUES(10001,'unknown',2,NULL);
INSERT INTO type VALUES(10002,'shadow',3,NULL);
