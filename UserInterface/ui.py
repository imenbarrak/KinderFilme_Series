from flask import Flask, render_template
from dash import Dash, dcc, html
import plotly.express as px
import pandas as pd
from fastapi import FastAPI
from threading import Thread
from pymongo import MongoClient
import Data.database_creation as do

# Create a Flask app
flask_app = Flask(__name__)

# Create a FastAPI app (we'll run this in a thread later)
fastapi_app = FastAPI()

# Sample Data for Visualization
df = pd.DataFrame({
    'Movie': ['Toy Story', 'Finding Nemo', 'Shrek', 'Frozen', 'The Lion King'],
    'Rating': [8.3, 8.1, 7.9, 7.5, 8.5],
    'Year': [1995, 2003, 2001, 2013, 1994]
})
db = do.get_db()
collection = db["children_movies"]
# Dash App setup
dash_app = Dash(__name__, server=flask_app, url_base_pathname='/dash/')

# Create a simple plot in Dash
fig = px.bar(df, x='Movie', y='Rating', title='Movie Ratings')

dash_app.layout = html.Div([
    html.H1("Children's Movies Ratings"),
    dcc.Graph(figure=fig)
])

# Route for Flask
@flask_app.route('/')
def home():
    return "<h1>Welcome to the Children's Movie App</h1><p>Visit the <a href='/dash/'>Dashboard</a> for movie ratings!</p>"

# FastAPI route for additional API functionality
@fastapi_app.get("/movie/{movie_name}")
def read_movie(movie_name: str):
    movie = df[df['Movie'].str.contains(movie_name, case=False)]
    return {"movie": movie.to_dict(orient="records")}

# Run the Flask app and FastAPI in separate threads
def run_flask():
    flask_app.run(debug=True, use_reloader=False, port=5000)

def run_fastapi():
    import uvicorn
    uvicorn.run(fastapi_app, host="127.0.0.1", port=8000)

if __name__ == "__main__":
    # Start Flask app in one thread
    Thread(target=run_flask).start()

    # Start FastAPI app in another thread
    Thread(target=run_fastapi).start()
