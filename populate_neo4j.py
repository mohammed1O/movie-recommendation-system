from neo4j import GraphDatabase
import pandas as pd
from pymongo import MongoClient
import sys

class Neo4jImporter:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        """Initialize Neo4j connection"""
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
        except Exception as e:
            print(f"Failed to connect to Neo4j: {e}")
            sys.exit(1)
            
    def close(self):
        """Close the Neo4j connection"""
        self.driver.close()

    def clear_database(self):
        """Clear all nodes and relationships from the database"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared")

    def create_constraints(self):
        """Create uniqueness constraints"""
        with self.driver.session() as session:
            # Create constraints if they don't exist
            try:
                session.run("CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")
                session.run("CREATE CONSTRAINT user_name IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE")
                print("Constraints created")
            except Exception as e:
                print(f"Error creating constraints: {e}")

    def import_movies_from_mongodb(self):
        """Import movies from MongoDB to Neo4j"""
        try:
            # Connect to MongoDB
            mongo_client = MongoClient()
            mongo_db = mongo_client["websitedb"]
            movies_collection = mongo_db.movies

            # Get all movies with minimal required fields
            movies = movies_collection.find({}, {
                "_id": 1,
                "title": 1,
                "genres": 1,
            })

            # Import movies in batches
            batch_size = 1000
            batch = []
            
            with self.driver.session() as session:
                for movie in movies:
                    # Prepare movie data
                    movie_data = {
                        "id": movie["_id"],
                        "title": movie["title"],
                        "genres": movie.get("genres", [])
                    }
                    batch.append(movie_data)
                    
                    if len(batch) >= batch_size:
                        self._create_movie_batch(session, batch)
                        batch = []
                        
                # Create any remaining movies
                if batch:
                    self._create_movie_batch(session, batch)
                    
            print("Movies imported from MongoDB")
            
        except Exception as e:
            print(f"Error importing movies from MongoDB: {e}")
        finally:
            mongo_client.close()

    def _create_movie_batch(self, session, batch):
        """Create a batch of movie nodes"""
        query = """
        UNWIND $movies AS movie
        MERGE (m:Movie {id: movie.id})
        SET m.title = movie.title,
            m.genres = movie.genres
        """
        session.run(query, movies=batch)

    def import_likes_from_csv(self, csv_path):
        """Import user likes from CSV file"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path, delimiter='\t')
            
            with self.driver.session() as session:
                for _, row in df.iterrows():
                    username = row['student_name']
                    # Convert movie_ids string to list of integers
                    movie_ids = [int(id.strip()) for id in str(row['movie_ids']).split(',')]
                    
                    # Create user and relationships
                    query = """
                    MERGE (u:User {username: $username})
                    WITH u
                    UNWIND $movie_ids AS movie_id
                    MATCH (m:Movie {id: movie_id})
                    MERGE (u)-[:LIKES]->(m)
                    """
                    session.run(query, username=username, movie_ids=movie_ids)
                    
            print("User likes imported from CSV")
            
        except Exception as e:
            print(f"Error importing likes from CSV: {e}")

def main():
    # Initialize importer
    importer = Neo4jImporter()
    
    try:
        # Clear existing data
        print("Clearing database...")
        importer.clear_database()
        
        # Create constraints
        print("Creating constraints...")
        importer.create_constraints()
        
        # Import movies from MongoDB
        print("Importing movies from MongoDB...")
        importer.import_movies_from_mongodb()
        
        # Import likes from CSV
        print("Importing likes from CSV...")
        importer.import_likes_from_csv("movies_likes.csv")
        
        print("Import completed successfully!")
        
    except Exception as e:
        print(f"An error occurred during import: {e}")
    finally:
        importer.close()

if __name__ == "__main__":
    main()