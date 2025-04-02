import os
import logging
import json
import time
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Always use mock mode for Render deployment
# You can change this to False once you have database access configured properly
MOCK_MODE = os.environ.get('MOCK_MODE', 'true').lower() == 'true'

def get_mock_data():
    """Generate mock data for testing without a database connection"""
    now = datetime.now()
    return {
        'timestamp': now.isoformat(),
        'refresh_interval': 300,  # 5 minutes in seconds
        'users': [
            {'username': 'Player1', 'points': 1250, 'updated_at': (now - timedelta(minutes=2)).isoformat()},
            {'username': 'Player2', 'points': 980, 'updated_at': (now - timedelta(minutes=3)).isoformat()},
            {'username': 'Player3', 'points': 875, 'updated_at': (now - timedelta(minutes=1)).isoformat()},
            {'username': 'Player4', 'points': 720, 'updated_at': (now - timedelta(minutes=4)).isoformat()},
            {'username': 'Player5', 'points': 650, 'updated_at': (now - timedelta(minutes=5)).isoformat()}
        ]
    }

# Connect to the database if not in mock mode (Not configured for Render yet)
db_manager = None
if not MOCK_MODE:
    try:
        logger.warning("Database connection not implemented yet - using mock data")
        MOCK_MODE = True
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        MOCK_MODE = True

@app.route('/')
def home():
    return jsonify({
        'status': 'running',
        'service': 'loyalty-points-leaderboard-api',
        'message': 'Use /api/leaderboard to access the leaderboard data',
        'mock_mode': MOCK_MODE
    })

@app.route('/api/leaderboard')
def leaderboard():
    try:
        minutes = request.args.get('minutes', 5, type=int)
        
        # Use mock data if in mock mode or if database connection fails
        if MOCK_MODE:
            data = get_mock_data()
            logger.info(f"Serving mock data for {minutes} minutes window")
            return jsonify(data)
        
        # Get active users from the database - This won't execute in Render deployment for now
        try:
            active_users = db_manager.get_active_users(minutes)
            
            # Format the response
            response = {
                'timestamp': datetime.now().isoformat(),
                'refresh_interval': 300,  # 5 minutes in seconds
                'users': active_users
            }
            
            logger.info(f"Serving leaderboard data with {len(active_users)} users for {minutes} minutes window")
            return jsonify(response)
        except Exception as db_error:
            logger.error(f"Database error: {str(db_error)}")
            # Fall back to mock data on error
            data = get_mock_data()
            return jsonify(data)
    
    except Exception as e:
        logger.error(f"Error serving leaderboard: {str(e)}")
        return jsonify({
            'error': 'Failed to retrieve leaderboard data',
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/health')
def health_check():
    return jsonify({
        'status': 'healthy', 
        'timestamp': datetime.now().isoformat(),
        'mock_mode': MOCK_MODE,
        'database_connected': db_manager is not None
    })

if __name__ == '__main__':
    # Get port from environment variable or use 8080 as default
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)