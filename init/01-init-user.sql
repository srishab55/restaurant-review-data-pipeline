CREATE DATABASE IF NOT EXISTS reviews_db;

CREATE USER IF NOT EXISTS 'review_user'@'%' IDENTIFIED WITH mysql_native_password BY 'review_pass';

GRANT ALL PRIVILEGES ON reviews_db.* TO 'review_user'@'%';

FLUSH PRIVILEGES;