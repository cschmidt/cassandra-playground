CREATE DATABASE IF NOT EXISTS cassandra_import_test;

USE cassandra_import_test;

CREATE TABLE IF NOT EXISTS T1 (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  description VARCHAR(40) NOT NULL,
  counter INT UNSIGNED
) ENGINE InnoDB;

INSERT INTO T1 (description, counter) values ("description0", 43);
