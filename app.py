from flask import Flask, render_template, request, session, redirect
import redis
import json
from datetime import datetime

# from .service_impl import (
from .service import (
    get_top_rated_movies,
    get_movie_details,
    get_recent_released_movies,
    get_recommendations_for_me,
    get_similar_movies,
    search_movie,
    get_movie_likes
)

# Setting up Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'

# Function for serializing datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Save the form data to the session object
        session["username"] = request.form["username"]
        return redirect("/")

    # Attempt to retrieve movies from Redis
    cached_top_rated = redis_client.get("top_rated_movies")
    if cached_top_rated:
        top_rated_movies = json.loads(cached_top_rated)
    else:
        top_rated_movies = get_top_rated_movies()
        redis_client.setex("top_rated_movies", 300, json.dumps(top_rated_movies, cls=DateTimeEncoder))

    cached_recent_movies = redis_client.get("recent_movies")
    if cached_recent_movies:
        recent_movies = json.loads(cached_recent_movies)
    else:
        recent_movies = get_recent_released_movies()
        redis_client.setex("recent_movies", 300, json.dumps(recent_movies, cls=DateTimeEncoder))

    if username := session.get("username"):
        cached_recommendations = redis_client.get(f"recommendations_{username}")
        if cached_recommendations:
            recommendations = json.loads(cached_recommendations)
        else:
            recommendations = get_recommendations_for_me(username)
            redis_client.setex(f"recommendations_{username}", 300, json.dumps(recommendations, cls=DateTimeEncoder))
    else:
        recommendations = []

    return render_template(
        "index.html",
        top_rated_movies=top_rated_movies,
        recent_movies=recent_movies,
        recommendations=recommendations,
    )


@app.route("/search")
def search_results():
    text = request.args["query"]
    result = search_movie(text)
    search_results = result.pop("searchResults")
    
    # Debug print to verify data before rendering
    #print("Debug - Template variables:")
    #print("releaseYearFacet:", result.get("releaseYearFacet"))
    #print("votes_facet:", result.get("votesFacet"))
    #print("genre_facet:", result.get("genreFacet"))
    
    return render_template(
        "list.html",
        search_results=search_results,
        votes_facet=result["votesFacet"],
        releaseYearFacet=result["releaseYearFacet"],
        genre_facet=result["genreFacet"],
    )


@app.route("/movie/<movie_id>")
def movie_details(movie_id):
    movie_id = int(movie_id)

    cached_movie = redis_client.get(f"movie_details_{movie_id}")
    if cached_movie:
        movie = json.loads(cached_movie)
    else:
        movie = get_movie_details(movie_id)
        if movie:
            redis_client.setex(f"movie_details_{movie_id}", 300, json.dumps(movie, cls=DateTimeEncoder))

    if movie and (username := session.get("username")):
        cached_likes = redis_client.get(f"likes_{username}_{movie_id}")
        if cached_likes:
            likes = json.loads(cached_likes)
        else:
            likes = get_movie_likes(username, movie_id)
            redis_client.setex(f"likes_{username}_{movie_id}", 300, json.dumps(likes, cls=DateTimeEncoder))
    else:
        likes = []

    if movie:
        cached_similar = redis_client.get(f"similar_movies_{movie_id}")
        if cached_similar:
            similar = json.loads(cached_similar)
        else:
            similar = get_similar_movies(movie_id, movie["genres"])
            redis_client.setex(f"similar_movies_{movie_id}", 300, json.dumps(similar, cls=DateTimeEncoder))
    else:
        similar = []

    return render_template("details.html", movie=movie, similar=similar, likes=likes)

if __name__ == "__main__":
    app.run(debug=True)