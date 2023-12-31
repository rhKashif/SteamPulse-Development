-- cleanup
DROP TABLE IF EXISTS game_genre_link;
DROP TABLE IF EXISTS game_developer_link;
DROP TABLE IF EXISTS game_publisher_link;
DROP TABLE IF EXISTS review;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS developer;
DROP TABLE IF EXISTS publisher;
DROP TABLE IF EXISTS game;
DROP TABLE IF EXISTS platform;
DROP TABLE IF EXISTS user_email;


-- tables with no foreign keys: platform, genre, publisher, developer

CREATE TABLE user_email(
    email_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    email TEXT NOT NULL UNIQUE
);


CREATE TABLE platform(
    platform_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    mac BOOLEAN NOT NULL,
    windows BOOLEAN NOT NULL,
    linux BOOLEAN NOT NULL,
    PRIMARY KEY (platform_id)

);


CREATE TABLE genre(
    genre_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    genre TEXT NOT NULL,
    user_generated BOOLEAN NOT NULL,
    PRIMARY KEY (genre_id)

);


CREATE TABLE developer(
    developer_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    developer_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (developer_id)
    

);


CREATE TABLE publisher(
    publisher_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    publisher_name TEXT NOT NULL UNIQUE,
    PRIMARY KEY (publisher_id)


);

-- Game references platform


-- Leaving game_id as regular int - not likely to hit the smallint cap if we're just working with new games, 
-- but there are more games on steam than the smallint cap, so if this ran for years or we expanded to grab
-- all games, smallint would cause issues.
CREATE TABLE game(
    game_id INT GENERATED ALWAYS AS IDENTITY,
    app_id INT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    release_date DATE NOT NULL,
    price FLOAT NOT NULL,
    sale_price FLOAT NOT NULL,
    platform_id SMALLINT NOT NULL,
    PRIMARY KEY (game_id),
    FOREIGN KEY (platform_id) REFERENCES platform(platform_id)

);

CREATE INDEX date_index ON game (release_date);
CREATE INDEX app_index ON game (game_id);

-- review references game

CREATE TABLE review(
    review_id INT GENERATED ALWAYS AS IDENTITY,
    sentiment FLOAT NOT NULL DEFAULT 0,
    review_text TEXT NOT NULL, 
    reviewed_at DATE NOT NULL,
    review_score INT NOT NULL DEFAULT 0,
    playtime_last_2_weeks INT NOT NULL,
    game_id INT NOT NULL,
    PRIMARY KEY (review_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id) 

);

CREATE UNIQUE INDEX review_constraint ON review (game_id, md5(review_text), review_score, reviewed_at, sentiment);

-- Linking tables for game with developer / publisher / genre


CREATE TABLE game_genre_link(
    genre_link_id INT GENERATED ALWAYS AS IDENTITY,
    game_id INT NOT NULL, 
    genre_id SMALLINT NOT NULL,
    PRIMARY KEY (genre_link_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id)

);


CREATE TABLE game_developer_link(
    developer_link_id INT GENERATED ALWAYS AS IDENTITY,
    game_id INT NOT NULL, 
    developer_id SMALLINT NOT NULL,
    PRIMARY KEY (developer_link_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (developer_id) REFERENCES developer(developer_id)

);


CREATE TABLE game_publisher_link(
    publisher_link_id INT GENERATED ALWAYS AS IDENTITY,
    game_id INT NOT NULL, 
    publisher_id SMALLINT NOT NULL,
    PRIMARY KEY (publisher_link_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id)

);


-- Seeding 

INSERT INTO platform(mac, windows, linux)
VALUES 
    (TRUE, TRUE, TRUE),
    (FALSE, FALSE, FALSE),
    
    (TRUE, FALSE, FALSE),
    (FALSE, TRUE, FALSE),
    (FALSE, FALSE, TRUE),
    
    (FALSE, TRUE, TRUE),
    (TRUE, FALSE, TRUE),
    (TRUE, TRUE, FALSE);
    

-- Hard to seed games / developers / publishers / genre
-- although we could pick 5 very popular games to manually enter and test our review loading against