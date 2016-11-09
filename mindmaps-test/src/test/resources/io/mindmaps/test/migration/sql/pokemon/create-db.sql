DROP TABLE IF EXISTS pokemon;

CREATE TABLE pokemon
(
  id               INT,
  identifier       VARCHAR(20),
  species_id       INT,
  height           INT,
  weight           INT,
  base_experience  INT,
  ordered          INT,
  is_default       INT,
  type1            INT,
  type2            INT
);

DROP TABLE IF EXISTS type;

CREATE TABLE type
(
  id              INT,
  identifier      VARCHAR(20),
  generation_id   INT,
  damage_class_id INT,
);

