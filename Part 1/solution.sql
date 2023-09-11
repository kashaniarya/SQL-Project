CREATE TABLE users (
    userid INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE movies (
    movieid INT PRIMARY KEY,
    title TEXT
);

CREATE TABLE taginfo (
    tagid INT PRIMARY KEY,
    content TEXT
);

CREATE TABLE genres (
    genreid INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE ratings (
    userid INT,
    movieid INT,
    rating NUMERIC 
        check (rating >= 0.0 and rating <= 5.0),
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users (userid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid)
);

CREATE TABLE tags (
    userid INT,
    movieid INT,
    tagid INT,
    timestamp BIGINT,
    PRIMARY KEY (userid, movieid, tagid),
    FOREIGN KEY (userid) REFERENCES users (userid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid),
    FOREIGN KEY (tagid) REFERENCES taginfo (tagid)
);

CREATE TABLE hasagenre (
    movieid INT,
    genreid INT,
    PRIMARY KEY (movieid, genreid),
    FOREIGN KEY (movieid) REFERENCES movies (movieid),
    FOREIGN KEY (genreid) REFERENCES genres (genreid)
);