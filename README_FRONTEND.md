# Real-Time People Dashboard

A beautiful real-time frontend dashboard that displays people records from Cassandra with live updates.

## Features

- 🔴 **Real-time updates** using WebSocket connections
- 📊 **Live statistics** showing total people and new additions
- 🎨 **Modern UI** with animations and hover effects
- 📱 **Responsive design** that works on all devices
- ⚡ **Fast loading** with efficient data fetching
- 🔔 **Notifications** for new person additions

## Architecture

```
Data Flow: Random User API → Kafka → Spark → Cassandra → Flask API → Frontend Dashboard
```

## Quick Start

### Method 1: Using Docker (Recommended)

1. **Start the complete pipeline:**
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready** (approximately 2-3 minutes)

3. **Open the dashboard:**
   - Open `frontend/index.html` in your browser
   - Or serve it using a simple HTTP server:
     ```bash
     cd frontend
     python -m http.server 3000
     ```
   - Visit: `http://localhost:3000`

### Method 2: Local Development

1. **Start backend services:**
   ```bash
   docker-compose up cassandra_db broker zookeeper -d
   ```

2. **Install backend dependencies:**
   ```bash
   cd backend
   pip install -r requirements.txt
   python app.py
   ```
   Backend will run on: `http://localhost:5001`

3. **Open frontend:**
   ```bash
   cd frontend
   # Open index.html in your browser or use:
   python -m http.server 3000
   ```

## Usage

### Starting Data Generation

1. **Access Airflow UI:**
   - URL: `http://localhost:8080` (Airflow)
   - Username: `admin`
   - Password: `admin`

2. **Enable the DAG:**
   - Find "user_automation" DAG
   - Toggle it ON to start generating people data

3. **Run Spark Streaming:**
   ```bash
   # In the project root
   python spark_stream.py
   ```

### Dashboard Features

- **Live Connection Status:** Top-right indicator shows WebSocket connection
- **Statistics Cards:** Real-time counts and metrics
- **People Grid:** Cards showing detailed person information
- **New Person Animations:** Special animations for newly added people
- **Notifications:** Toast notifications for new additions

## Troubleshooting

### Backend Issues

1. **Cannot connect to Cassandra:**
   ```bash
   # Check if Cassandra is running
   docker ps | grep cassandra
   
   # Check Cassandra logs
   docker logs cassandra
   ```

2. **No data showing:**
   ```bash
   # Verify keyspace and table exist
   docker exec -it cassandra cqlsh
   DESCRIBE KEYSPACES;
   USE spark_streams;
   DESCRIBE TABLES;
   SELECT COUNT(*) FROM created_users;
   ```

### Frontend Issues

1. **CORS errors:**
   - Ensure backend is running with CORS enabled
   - Check browser console for specific errors

2. **WebSocket connection failed:**
   - Verify backend is running on port 5000
   - Check firewall settings

### Data Pipeline Issues

1. **No new data:**
   - Check if Airflow DAG is running
   - Verify Kafka is receiving messages
   - Ensure Spark streaming job is active

## API Endpoints

- `GET /api/people` - Fetch all people records
- WebSocket events:
  - `connect` - Client connection established
  - `new_person` - New person added to database
  - `disconnect` - Client disconnected

## Technical Details

### Backend Stack
- **Flask** - Web framework
- **Flask-SocketIO** - WebSocket support
- **Cassandra Driver** - Database connectivity
- **Flask-CORS** - Cross-origin support

### Frontend Stack
- **Vanilla JavaScript** - No framework dependencies
- **Socket.IO Client** - Real-time communication
- **CSS Grid & Flexbox** - Modern responsive layout
- **CSS Animations** - Smooth user experience

### Database Schema
```sql
CREATE TABLE spark_streams.created_users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    post_code TEXT,
    email TEXT,
    username TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
```

## Performance Notes

- Dashboard polls for changes every 2 seconds
- WebSocket provides instant updates for new records
- Frontend efficiently manages DOM updates
- Backend uses connection pooling for Cassandra

## Customization

### Styling
- Edit `frontend/index.html` CSS section
- Modify color scheme in CSS variables
- Adjust grid layout for different screen sizes

### Features
- Add new person fields in `createPersonCard()` function
- Modify statistics in `updateStats()` method
- Customize notifications in `showNotification()` method

## Monitoring

- **Backend logs:** Check Flask console output
- **Frontend logs:** Use browser developer tools
- **Database stats:** Use Cassandra's nodetool
- **WebSocket status:** Check connection indicator in UI