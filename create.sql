CREATE TABLE kdramas
(
  kdrama_id INT NOT NULL,
  kdrama_name VARCHAR(50) NOT NULL,
  number_of_episodes INT NOT NULL,
  airing_date DATE NOT NULL,
  episode_run_time INT NOT NULL,
  PRIMARY KEY (kdrama_id)
);

CREATE TABLE kdramas_genre
(
  genre_name VARCHAR(20) NOT NULL,
  kdrama_id INT NOT NULL,
  PRIMARY KEY (genre_name, kdrama_id),
  FOREIGN KEY (kdrama_id) REFERENCES kdramas(kdrama_id)
);

CREATE TABLE actors
(
  actor_id INT NOT NULL,
  actor_name VARCHAR(25) NOT NULL,
  gender INT NOT NULL,
  PRIMARY KEY (actor_id)
);

CREATE TABLE kdramas_actor
(
  kdrama_id INT NOT NULL,
  actor_id INT NOT NULL,
  PRIMARY KEY (kdrama_id, actor_id),
  FOREIGN KEY (kdrama_id) REFERENCES kdramas(kdrama_id),
  FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
);