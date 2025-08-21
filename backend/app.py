from flask import Flask, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from cassandra.cluster import Cluster
import threading
import time
import json
import logging

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

def get_cassandra_session():
    try:
        import os
        cassandra_host = os.getenv('CASSANDRA_HOST', 'localhost')
        cluster = Cluster([cassandra_host])
        session = cluster.connect()
        session.set_keyspace('spark_streams')
        return session
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None

@app.route('/api/people', methods=['GET'])
def get_all_people():
    session = get_cassandra_session()
    if not session:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        rows = session.execute("SELECT * FROM created_users")
        people = []
        for row in rows:
            person = {
                'id': str(row.id) if row.id else None,
                'first_name': row.first_name,
                'last_name': row.last_name,
                'gender': row.gender,
                'address': row.address,
                'post_code': row.post_code,
                'email': row.email,
                'username': row.username,
                'registered_date': row.registered_date,
                'phone': row.phone,
                'picture': row.picture
            }
            people.append(person)
        
        return jsonify(people)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return jsonify({'error': 'Failed to fetch data'}), 500

def monitor_cassandra_changes():
    session = get_cassandra_session()
    if not session:
        return
    
    last_count = 0
    
    while True:
        try:
            result = session.execute("SELECT COUNT(*) FROM created_users")
            current_count = result.one()[0]
            
            if current_count > last_count:
                rows = session.execute("SELECT * FROM created_users LIMIT 1 ALLOW FILTERING")
                new_person = None
                for row in rows:
                    new_person = {
                        'id': str(row.id) if row.id else None,
                        'first_name': row.first_name,
                        'last_name': row.last_name,
                        'gender': row.gender,
                        'address': row.address,
                        'post_code': row.post_code,
                        'email': row.email,
                        'username': row.username,
                        'registered_date': row.registered_date,
                        'phone': row.phone,
                        'picture': row.picture
                    }
                    break
                
                if new_person:
                    socketio.emit('new_person', new_person)
                
                last_count = current_count
            
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error monitoring changes: {e}")
            time.sleep(5)

@socketio.on('connect')
def handle_connect():
    print('Client connected')
    emit('connected', {'data': 'Connected to real-time updates'})

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    monitor_thread = threading.Thread(target=monitor_cassandra_changes, daemon=True)
    monitor_thread.start()
    
    socketio.run(app, host='0.0.0.0', port=5001, debug=True)