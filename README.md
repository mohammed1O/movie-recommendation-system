# movie-recommendation-system
his project is a backend server for a movie recommendation website built with Flask. It lets users search for movies, see movie details, and get personalized movie recommendations.
Movie Recommendation Backend
This project is a backend server for a movie recommendation website built with Flask. It lets users search for movies, see movie details, and get personalized movie recommendations.

Main Parts:
1. Flask App (backend.py)
Manages the website API.

Shows top-rated movies, new releases, and user recommendations on the home page.

Lets users search movies by text.

Shows movie details, similar movies, and how many people liked the movie.

Uses sessions to remember the logged-in user.

Uses Redis caching to make responses faster.

Converts dates and times to JSON format.

2. Neo4j Data Importer (neo4j_importer.py)
Connects to the Neo4j database.

Clears old data and sets unique rules for movies and users.

Imports movies from MongoDB to Neo4j.

Imports user "likes" from a CSV file and links them in Neo4j.

Processes data in batches for better performance.

3. Data Queries (movie_queries.py)
Searches movies by title using MongoDB text search.

Gets top 25 movies by rating with enough votes.

Gets recent movies released in the last 2 years with enough votes.

Gets detailed info about a movie.

Finds similar movies based on genres.

Finds other users who liked the same movie using Neo4j.

Makes personalized recommendations by finding similar users and movies they like.

Technologies Used:
Flask (Python) — for the backend server.

MongoDB — stores movie data.

Neo4j — graph database for user and likes relationships.

Redis — cache to speed up the app.

JSON — for data exchange.

Purpose:
To build a smart and fast movie website backend that helps users find and get movie recommendations based on their tastes.
