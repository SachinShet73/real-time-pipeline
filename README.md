# 🔴 Real-Time People Dashboard

A complete real-time data pipeline that fetches people data from Random User API, processes it through Kafka and Spark, stores in Cassandra, and displays it on a beautiful web dashboard with live updates.

## 🏗️ Architecture

```
Random User API → Airflow → Kafka → Spark Streaming → Cassandra → Flask API → React Dashboard
```

### Components:
- **Data Source**: Random User API
- **Orchestration**: Apache Airflow
- **Message Queue**: Apache Kafka
- **Stream Processing**: Apache Spark
- **Database**: Apache Cassandra
- **Backend API**: Flask with WebSocket support
- **Frontend**: Vanilla JavaScript with real-time updates
- **Web Server**: Nginx
- **Monitoring**: Confluent Control Center

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB+ RAM recommended
- Ports 3000, 5001, 8080, 9021, 9042, 9092 available

### Option 1: Development Mode (Recommended)
```bash
./start-dev.sh
```

### Option 2: Production Mode
```bash
./start-prod.sh
```

### Option 3: Manual Start
```bash
# Development
docker-compose up --build

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up --build
```

## 📱 Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Frontend Dashboard** | http://localhost:3000 | None |
| **Airflow UI** | http://localhost:8080 | admin/admin |
| **Confluent Control Center** | http://localhost:9021 | None |
| **Backend API** | http://localhost:5001 | None |

## 🔄 Starting Data Flow

1. **Access Airflow**: Go to http://localhost:8080
2. **Enable DAG**: Toggle ON the "user_automation" DAG
3. **Trigger DAG**: Click the play button to start data generation
4. **View Dashboard**: Go to http://localhost:3000 to see real-time updates

## 🐳 Docker Services

| Service | Container | Purpose |
|---------|-----------|---------|
| `people-dashboard-frontend` | people-frontend | Nginx web server with React app |
| `people-dashboard-backend` | people-backend | Flask API with WebSocket support |
| `spark-streaming` | spark-streaming | Processes Kafka messages to Cassandra |
| `cassandra_db` | cassandra | NoSQL database for storing people data |
| `broker` | broker | Kafka message broker |
| `zookeeper` | zookeeper | Kafka coordination service |
| `webserver` | airflow-webserver | Airflow web interface |
| `scheduler` | airflow-scheduler | Airflow task scheduler |
| `postgres` | postgres | Airflow metadata database |
| `spark-master` | spark-master | Spark cluster master |
| `spark-worker` | spark-worker | Spark cluster worker |
| `schema-registry` | schema-registry | Kafka schema management |
| `control-center` | control-center | Confluent monitoring UI |

## 🛠️ Development

### File Structure
```
real-time-pipeline/
├── frontend/                 # React frontend
│   ├── Dockerfile
│   ├── nginx.conf
│   ├── index.html
│   └── app.js
├── backend/                  # Flask backend
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
├── spark-docker/            # Spark streaming
│   ├── Dockerfile
│   └── spark_stream.py
├── dags/                    # Airflow DAGs
│   └── kafka_stream.py
├── docker-compose.yml       # Main compose file
├── docker-compose.prod.yml  # Production overrides
├── docker-compose.override.yml # Development overrides
├── .env                     # Environment variables
├── start-dev.sh            # Development startup
└── start-prod.sh           # Production startup
```

### Environment Variables
Edit `.env` file to customize configuration:

```env
CASSANDRA_HOST=cassandra
KAFKA_BOOTSTRAP_SERVERS=broker:29092
BACKEND_PORT=5001
FRONTEND_PORT=3000
DEBUG=false
```

### Adding New Features

1. **Backend Changes**: Modify `backend/app.py`
2. **Frontend Changes**: Modify `frontend/app.js` or `frontend/index.html`
3. **Spark Processing**: Modify `spark-docker/spark_stream.py`
4. **Data Generation**: Modify `dags/kafka_stream.py`

## 🔍 Monitoring & Debugging

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f people-backend
docker-compose logs -f spark-streaming
```

### Health Checks
```bash
# Check service status
docker-compose ps

# Check specific service health
curl http://localhost:3000/health
curl http://localhost:5001/api/people
```

### Database Access
```bash
# Access Cassandra
docker exec -it cassandra cqlsh

# View data
SELECT COUNT(*) FROM spark_streams.created_users;
SELECT * FROM spark_streams.created_users LIMIT 5;
```

### Kafka Debugging
```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# View messages
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic users_created --from-beginning --max-messages 5
```

## 🚨 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check what's using ports
   lsof -i :3000
   lsof -i :8080
   
   # Kill processes if needed
   sudo kill -9 <PID>
   ```

2. **Service Won't Start**
   ```bash
   # Rebuild containers
   docker-compose down
   docker-compose up --build
   
   # Check logs
   docker-compose logs <service-name>
   ```

3. **No Data Flowing**
   - Ensure Airflow DAG is enabled and running
   - Check Kafka topic has messages
   - Verify Spark streaming is processing
   - Confirm Cassandra connection

4. **Frontend Can't Connect**
   - Check backend is running on port 5001
   - Verify WebSocket connection in browser console
   - Ensure CORS is properly configured

### Performance Tuning

1. **Increase Memory**: Add to docker-compose.yml
   ```yaml
   deploy:
     resources:
       limits:
         memory: 2G
   ```

2. **Spark Tuning**: Modify spark-docker/spark_stream.py
   ```python
   .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   ```

## 📊 Features

### Real-Time Dashboard
- ✅ Live people data display
- ✅ Real-time statistics
- ✅ WebSocket connections
- ✅ Responsive design
- ✅ Connection status indicator
- ✅ Toast notifications

### Data Pipeline
- ✅ Random User API integration
- ✅ Kafka message streaming
- ✅ Spark stream processing
- ✅ Cassandra data storage
- ✅ Airflow orchestration

### Production Ready
- ✅ Docker containerization
- ✅ Health checks
- ✅ Nginx web server
- ✅ Environment configuration
- ✅ Logging and monitoring
- ✅ Graceful shutdowns

## 🔒 Security

### Production Considerations
- Change default passwords in `.env`
- Use proper SSL certificates
- Implement authentication
- Restrict network access
- Regular security updates

### Current Security Features
- CORS protection
- XSS protection headers
- Content type validation
- Network isolation via Docker

## 📈 Scaling

### Horizontal Scaling
- Add more Spark workers
- Scale Kafka partitions
- Use Cassandra cluster
- Load balance frontend

### Vertical Scaling
- Increase container memory
- Optimize Spark configurations
- Database query optimization

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Make changes and test
4. Submit pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Apache Kafka, Spark, Cassandra, Airflow teams
- Random User API for test data
- Docker and containerization community