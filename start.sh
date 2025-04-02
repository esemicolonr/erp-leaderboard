#!/bin/bash
export FLASK_APP=app.py
export FLASK_ENV=development
export MOCK_MODE=true

# Start the Flask development server
flask run --host=0.0.0.0 --port=8080