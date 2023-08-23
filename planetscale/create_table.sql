CREATE TABLE stats (
  id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
  date varchar(255) UNIQUE NOT NULL,
  milk_count INT UNSIGNED,
  milk_ml FLOAT UNSIGNED,
  unchi_count INT UNSIGNED,
  unchi_amount INT UNSIGNED,
  age_of_month INT UNSIGNED
);