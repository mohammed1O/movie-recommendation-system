import datetime
import pymongo
from pymongo import MongoClient
import pprint
from neo4j import GraphDatabase

client = MongoClient()
db = client["websitedb"]
#db = client.websitesdb
#print(db)
collection = db.movies
#print(collection)

#####List collection#####
#list_collection=db.list_collection_names()
#print(list_collection)

######Print a document######
pprint.pprint(collection.find_one())

def search_movie(text):

    try:


        # Create text index if it doesn't exist
        collection.create_index([("title", "text")])
        
        pipeline = [
            # Text search stage
            {
                "$match": {
                    "$text": {"$search": text}
                }
            },
            # Facet to get both search results and aggregations
            {
                "$facet": {
                    "searchResults": [
                        {
                            "$addFields": {
                                "score": {"$meta": "textScore"}
                            }
                        },
                        {
                            "$sort": {
                                "score": -1,
                                "popularity": -1,
                                "vote_average": -1
                            }
                        },
                        {
                            "$project": {
                                "_id": 1,
                                "poster_path": 1,
                                "release_date": 1,
                                "score": 1,
                                "title": 1,
                                "vote_average": 1,
                                "vote_count": 1
                            }
                        },
                        {"$limit": 20}
                    ],
                    "genreFacet": [
                        {"$unwind": "$genres"},
                        {
                            "$group": {
                                "_id": "$genres",
                                "count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"count": -1}}
                    ],
                    "releaseYearFacet": [
                        {
                            "$match": {
                                "release_date": {"$exists": True, "$ne": None}
                            }
                        },
                        {
                            "$group": {
                                "_id": {"$year": "$release_date"},
                                "count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"_id": -1}},
                        {"$limit": 10}
                    ],
                    "votesFacet": [
                        {
                            "$group": {
                                "_id": {
                                    "$switch": {
                                        "branches": [
                                            {"case": {"$lte": ["$vote_count", 0]}, "then": 0},
                                            {"case": {"$lte": ["$vote_count", 1]}, "then": 1},
                                            {"case": {"$lte": ["$vote_count", 16]}, "then": 16},
                                            {"case": {"$lte": ["$vote_count", 19155]}, "then": 19155}
                                        ],
                                        "default": 19155
                                    }
                                },
                                "count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"_id": 1}}
                    ]
                }
            }
        ]

        # Execute aggregation
        result = list(collection.aggregate(pipeline))[0]
        
        # Transform vote facets to include descriptions
        vote_descriptions = {
            0: "Unrated (0 votes)",
            1: "Barely rated (1 vote)",
            16: "Moderately rated (up to 16 votes)",
            19155: "Highly rated (up to 19155 votes)"
        }
        
        if "votesFacet" in result:
            result["votesFacet"] = [
                {
                    "_id": vote_descriptions[facet["_id"]],
                    "count": facet["count"]
                }
                for facet in result["votesFacet"]
            ]

        # Ensure all facets exist
        if "searchResults" not in result:
            result["searchResults"] = []
        if "genreFacet" not in result:
            result["genreFacet"] = []
        if "releaseYearFacet" not in result:
            result["releaseYearFacet"] = []
        if "votesFacet" not in result:
            result["votesFacet"] = []

        return result

    except Exception as e:
        print(f"Error searching movies: {e}")
        return {
            "searchResults": [],
            "genreFacet": [],
            "releaseYearFacet": [],
            "votesFacet": []
        }





def get_top_rated_movies():

    try:

        
        # Query movies with vote_count > 5000, sort by vote_average descending
        # Limit to 25 results
        top_movies = collection.find(
            {"vote_count": {"$gt": 5000}},
            projection={
                "_id": 1,
                "poster_path": 1,
                "release_date": 1,
                "title": 1,
                "vote_average": 1,
                "vote_count": 1
            }
        ).sort("vote_average", pymongo.DESCENDING).limit(25)
        
        # Convert cursor to list and ensure consistent format
        result = []
        for movie in top_movies:
            result.append({
                "_id": movie["_id"],
                "poster_path": movie.get("poster_path", ""),
                "release_date": movie.get("release_date"),
                "title": movie.get("title", ""),
                "vote_average": movie.get("vote_average", 0),
                "vote_count": movie.get("vote_count", 0)
            })
            
        return result
        
    except Exception as e:
        print(f"Error retrieving top rated movies: {e}")
        return []


"""
def get_top_rated_movies():

    try:
        # Define the query: movies with more than 5000 votes
        query = {"vote_count": {"$gt": 5000}}

        # Define the fields to retrieve (you can modify them as needed)
        projection = {
            "title": 1,
            "vote_average": 1,
            "vote_count": 1,
            "release_date": 1,
            "poster_path": 1  # For example, if you need the poster path
        }

        # Execute the query: sort by descending rating, limit results to 25
        top_movies_cursor = collection.find(query, projection).sort("vote_average", DESCENDING).limit(25)

        # Convert the results to a list
        top_movies = list(top_movies_cursor)

        return top_movies
    except Exception as e:
        print(f"An error occurred while fetching top rated movies: {e}")
        return []
"""

def get_recent_released_movies():

    try:


        # Get current date
        current_date = datetime.datetime.now()
        
        # Calculate date 3 months ago
        three_months_ago = current_date - datetime.timedelta(days=720)
        
        # Query for recent movies with at least 50 votes
        recent_movies = collection.find(
            {
                "vote_count": {"$gte": 50},
                "release_date": {
                    "$gte": three_months_ago,
                    "$lte": current_date
                }
            },
            projection={
                "_id": 1,
                "poster_path": 1,
                "release_date": 1,
                "title": 1,
                "vote_average": 1,
                "vote_count": 1
            }
        ).sort("release_date", pymongo.DESCENDING).limit(25)
        
        # Convert cursor to list and ensure consistent format
        result = []
        for movie in recent_movies:
            result.append({
                "_id": movie["_id"],
                "poster_path": movie.get("poster_path", ""),
                "release_date": movie.get("release_date"),
                "title": movie.get("title", ""),
                "vote_average": movie.get("vote_average", 0),
                "vote_count": movie.get("vote_count", 0)
            })
            
        return result
        
    except Exception as e:
        print(f"Error retrieving recent movies: {e}")
        return []



def get_movie_details(movie_id):
 
    try:

        
        # Query for the specific movie
        movie = collection.find_one(
            {"_id": movie_id},
            projection={
                "_id": 1,
                "title": 1,
                "genres": 1,
                "overview": 1,
                "poster_path": 1,
                "release_date": 1,
                "tagline": 1,
                "vote_average": 1,
                "vote_count": 1
            }
        )
        
        # Return None if movie not found
        if not movie:
            return None
            
        # Ensure the return format matches the expected structure
        return {
            "_id": movie["_id"],
            "genres": movie.get("genres", []),
            "overview": movie.get("overview", ""),
            "poster_path": movie.get("poster_path", ""),
            "release_date": movie.get("release_date"),
            "tagline": movie.get("tagline", ""),
            "title": movie.get("title", ""),
            "vote_average": movie.get("vote_average", 0),
            "vote_count": movie.get("vote_count", 0)
        }
        
    except Exception as e:
        print(f"Error retrieving movie details: {e}")
        return None



def get_similar_movies(movie_id, genres):

    try:

        
        # Build the query
        query = {
            "_id": {"$ne": movie_id},  # Exclude the reference movie
            "vote_count": {"$gte": 500},  # At least 500 votes
            "genres": {"$in": genres}  # Match at least one genre
        }
        
        # Use aggregation pipeline to calculate genre matches and sort
        pipeline = [
            {"$match": query},
            {
                "$addFields": {
                    "genres": {  # Count matching genres
                        "$size": {
                            "$setIntersection": ["$genres", genres]
                        }
                    }
                }
            },
            {"$sort": {
                "genres": -1,  # Sort by number of matching genres (descending)
                "vote_average": -1  # Then by rating (descending)
            }},
            {"$limit": 10},  # Limit to 10 results as specified
            {
                "$project": {
                    "_id": 1,
                    "genres": 1,  # This will now be the count of matching genres
                    "poster_path": 1,
                    "release_date": 1,
                    "title": 1,
                    "vote_average": 1,
                    "vote_count": 1
                }
            }
        ]
        
        # Execute aggregation pipeline
        similar_movies = collection.aggregate(pipeline)
        
        # Convert cursor to list and ensure consistent format
        result = []
        for movie in similar_movies:
            result.append({
                "_id": movie["_id"],
                "genres": movie["genres"],  # Number of matching genres
                "poster_path": movie.get("poster_path", ""),
                "release_date": movie.get("release_date"),
                "title": movie.get("title", ""),
                "vote_average": movie.get("vote_average", 0),
                "vote_count": movie.get("vote_count", 0)
            })
            
        return result
        
    except Exception as e:
        print(f"Error retrieving similar movies: {e}")
        return []



def get_movie_likes(username, movie_id):
    #print(f"Debug - username: {username}, movie_id: {movie_id}, type(movie_id): {type(movie_id)}")
    #print(f"Getting likes with username: {username}")
    try:
        # Connect to Neo4j
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")  # Make sure to use your actual Neo4j password
        )
        
        with driver.session() as session:
            # Query to find other users who like the specified movie
            result = session.run("""
                MATCH (u:User)-[:LIKES]->(m:Movie {id: $movie_id})
                WHERE u.username <> $username
                RETURN u.username AS username
                ORDER BY u.username
                """, 
                movie_id=movie_id,
                username=username
            )
            
            # Convert results to list of usernames
            usernames = [record["username"] for record in result]
            return usernames
            
    except Exception as e:
        print(f"Error getting movie likes: {e}")
        return []
    finally:
        if 'driver' in locals():
            driver.close()


""""""
def get_recommendations_for_me(username):
    """
    Get movie recommendations for a user based on similar users' preferences using an optimized single-query approach.
    
    Args:
        username (str): The username to get recommendations for
        
    Returns:
        list: List of recommended movies with their details, ordered by recommendation strength
        Example:
        [
            {
                "_id": 496243,
                "poster_path": "/7IiTTgloJzvGI1TAYymCfbfl3vT.jpg",
                "release_date": datetime.datetime(2019, 5, 30, 0, 0),
                "title": "Parasite",
                "vote_average": 8.515,
                "vote_count": 16430,
            }
        ]
    """
    try:
        # Connect to Neo4j
        neo4j_driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")
        )
        
        # Connect to MongoDB
        mongo_client = MongoClient()
        mongo_db = mongo_client["websitedb"]
        movies_collection = mongo_db.movies
        
        # Get recommended movie IDs using optimized Neo4j query
        with neo4j_driver.session() as session:
            result = session.run("""
                // Find active user's liked movies
                MATCH (active:User {username: $username})-[:LIKES]->(m:Movie)
                WITH active, collect(m.id) as active_movies

                // Find other users and their liked movies
                MATCH (other:User)-[:LIKES]->(m:Movie)
                WHERE other.username <> active.username
                WITH active, active_movies, other, collect(m.id) as other_movies

                // Calculate Jaccard similarity in a single step
                WITH 
                    active,
                    other,
                    active_movies,
                    other_movies,
                    size([x IN active_movies WHERE x IN other_movies]) as intersection_size,
                    size(active_movies + [x IN other_movies WHERE NOT x IN active_movies]) as union_size,
                    toFloat(size([x IN active_movies WHERE x IN other_movies])) / 
                    size(active_movies + [x IN other_movies WHERE NOT x IN active_movies]) as jaccard_index

                // Filter and rank similar users
                WHERE jaccard_index > 0
                WITH active, other, jaccard_index
                ORDER BY jaccard_index DESC
                LIMIT 3

                // Get movie recommendations from similar users
                WITH collect(other) as similar_users, active
                UNWIND similar_users as neighbor
                MATCH (neighbor)-[:LIKES]->(recommended:Movie)
                WHERE NOT (active)-[:LIKES]->(recommended)

                // Group and rank recommendations
                WITH recommended.id as movie_id, count(*) as num_likes
                WHERE num_likes > 0
                ORDER BY num_likes DESC
                LIMIT 10
                RETURN movie_id, num_likes
            """, username=username)
            
            # Extract movie IDs while preserving order
            movie_ids = [record["movie_id"] for record in result]
        
        # Get detailed movie information from MongoDB
        recommendations = []
        if movie_ids:
            # Create an index on _id if it doesn't exist
            movies_collection.create_index([("_id", 1)])
            
            # Fetch movies and create a mapping to preserve order
            movies_cursor = movies_collection.find(
                {"_id": {"$in": movie_ids}},
                projection={
                    "_id": 1,
                    "poster_path": 1,
                    "release_date": 1,
                    "title": 1,
                    "vote_average": 1,
                    "vote_count": 1
                }
            )
            
            # Create a mapping of movie_id to movie details
            movie_dict = {
                movie["_id"]: {
                    "_id": movie["_id"],
                    "poster_path": movie.get("poster_path", ""),
                    "release_date": movie.get("release_date"),
                    "title": movie.get("title", ""),
                    "vote_average": movie.get("vote_average", 0),
                    "vote_count": movie.get("vote_count", 0)
                }
                for movie in movies_cursor
            }
            
            # Maintain the order from Neo4j recommendations
            recommendations = [
                movie_dict[movie_id]
                for movie_id in movie_ids
                if movie_id in movie_dict
            ]

        return recommendations

    except Exception as e:
        print(f"Error getting recommendations: {e}")
        return []
    
    finally:
        # Ensure connections are properly closed
        if 'neo4j_driver' in locals():
            neo4j_driver.close()
        if 'mongo_client' in locals():
            mongo_client.close()
            