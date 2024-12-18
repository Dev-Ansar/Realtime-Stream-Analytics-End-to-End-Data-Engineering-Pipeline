import random
import datetime

class EventGenerator:
    def __init__(self):
        self.movie_titles = ["Inception", "The Dark Knight", "Interstellar", "Iron Man 1"]
        self.user_ids = [f"user{random.randint(1, 5)}" for _ in range(5)]  # Limited users
        self.sessions = {}  # Store ongoing sessions for each user

    def generate_video_playback_event(self):
        user_id = random.choice(self.user_ids)
        session_id = None

        # Check if the user has an ongoing session or needs a new one
        if user_id in self.sessions and self.sessions[user_id]['status'] != 'finished':
            session_id = self.sessions[user_id]['sessionId']
        else:
            # Start a new session
            session_id = f"session{random.randint(1, 1000)}"
            self.sessions[user_id] = {
                'sessionId': session_id,
                'movieId': random.choice(self.movie_titles),
                # 'startTimestamp': datetime.datetime.utcnow().isoformat() + "Z",
                'startTimestamp': datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                'status': 'playing',  # Initially playing
                'currentPosition': 0,  # Movie starts at 0 seconds
            }

        # Generate action (play, pause, resume, stop)
        action = random.choice(["play", "pause", "resume", "stop"])
        current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        # Action-specific handling
        if action == "play":
            self.sessions[user_id]['status'] = 'playing'
            self.sessions[user_id]['startTimestamp'] = current_timestamp  # Start time for play
        elif action == "pause":
            self.sessions[user_id]['status'] = 'paused'
            self.sessions[user_id]['pauseTimestamp'] = current_timestamp
            self.sessions[user_id]['currentPosition'] = random.randint(0, 7200)  # Random position in movie
        elif action == "resume":
            self.sessions[user_id]['status'] = 'playing'
            # Calculate time between pause and resume
            self.sessions[user_id]['resumeTimestamp'] = current_timestamp
        elif action == "stop":
            self.sessions[user_id]['status'] = 'finished'
            self.sessions[user_id]['stopTimestamp'] = current_timestamp
            self.sessions[user_id]['currentPosition'] = 7200  # Assuming the movie ends

        # Return the event structure
        event = {
            "eventType": "Playback",
            "timestamp": current_timestamp,
            "userId": user_id,
            "movieId": self.sessions[user_id]['movieId'],
            "sessionId": session_id,
            "action": action,
            "currentPosition": self.sessions[user_id]['currentPosition'],
        }

        return event

    def generate_search_event(self):
        user_id = random.choice(self.user_ids)
        search_query = random.choice(["action movies", "comedy films", "romantic movies", "horror films"])
        filters = {"genre": random.choice(["Action", "Comedy", "Romance", "Horror"]), 
                   "year": random.randint(1990, 2023)}  # Optional filters
        results_count = random.randint(10, 50)  # Number of search results
        current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        event = {
            "eventType": "Search",
            "timestamp": current_timestamp,
            "userId": user_id,
            "query": search_query,
            "filters": filters,
            "resultsCount": results_count,
        }

        return event

    def generate_like_dislike_event(self):
        user_id = random.choice(self.user_ids)
        movie_id = random.choice(self.movie_titles)
        action = random.choice(["like", "dislike"])  # Randomly choose like or dislike
        current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        event = {
            "eventType": "LikeDislike",
            "timestamp": current_timestamp,
            "userId": user_id,
            "movieId": movie_id,
            "action": action,
        }

        return event

    def generate_navigation_event(self):
        user_id = random.choice(self.user_ids)
        from_page = random.choice(["/home", "/search", "/movie-details", "/user-profile"])
        
        # Ensure 'toPage' is different from 'fromPage'
        to_page = random.choice([page for page in ["/home", "/search", "/movie-details", "/user-profile"] if page != from_page])

        interaction_type = random.choice(["click", "scroll", "swipe"])  # Simulate interactions
        current_timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        event = {
            "eventType": "Navigation",
            "timestamp": current_timestamp,
            "userId": user_id,
            "fromPage": from_page,
            "toPage": to_page,
            "interactionType": interaction_type,
        }

        return event

