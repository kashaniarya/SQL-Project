
-- query 1
CREATE TABLE query1(name, moviecount) as
SELECT genres.name, COUNT(genres.name) 
FROM hasagenre
NATURAL JOIN genres
NATURAL JOIN movies
GROUP BY genres.name;

-- query 2
CREATE TABLE query2(name, rating) as
SELECT genres.name, AVG(ratings.rating) 
FROM movies 
NATURAL JOIN genres
NATURAL JOIN ratings
NATURAL JOIN hasagenre
GROUP BY genres.name;

-- query 3
CREATE TABLE query3(title, CountOfRatings) as 
SELECT movies.title, count(ratings.rating)
FROM movies
NATURAL JOIN ratings 
GROUP BY movies.title
HAVING COUNT(*) >= 10;

-- query 4
CREATE TABLE query4(movieid, title) as 
SELECT movieid, title 
FROM movies 
NATURAL JOIN genres 
NATURAL JOIN hasagenre 
WHERE genres.name = 'Comedy'
GROUP BY movieid;

-- query 5
CREATE TABLE query5(title, average) as 
SELECT movies.title, AVG(ratings.rating) 
FROM movies 
NATURAL JOIN ratings 
GROUP BY movies.title;

-- query 6
CREATE TABLE query6(average) as 
SELECT AVG(ratings.rating)
FROM movies
NATURAL JOIN ratings 
NATURAL JOIN genres 
NATURAL JOIN hasagenre 
where genres.name = 'Comedy';

-- query 7
CREATE TABLE query7(average) as
SELECT AVG(ratings.rating)
FROM ratings
INNER JOIN (SELECT hasagenre.movieid
            FROM hasagenre  
            NATURAL JOIN genres
            WHERE genres.name IN ('Comedy', 'Romance')
            GROUP BY hasagenre.movieid
            HAVING COUNT(DISTINCT genres.name) = 2
) m ON ratings.movieid = m.movieid;

-- query 8 
CREATE TABLE query8 AS
SELECT AVG(r.rating) AS average 
FROM ratings r 
WHERE r.movieid IN 
((SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Romance') 
 EXCEPT 
 (SELECT h.movieid 
  FROM genres g, hasagenre h   
  WHERE g.genreid = h.genreid AND g.name = 'Comedy'));

  -- query 9
CREATE TABLE query9 AS
SELECT movieid, rating
FROM ratings
WHERE ratings.userid = :v1;


















