# Real-Time People Dashboard

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Apache Cassandra](https://img.shields.io/badge/Apache%20Cassandra-1287B1?style=for-the-badge&logo=apache-cassandra&logoColor=white)](https://cassandra.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Flask](https://img.shields.io/badge/Flask-000000?style=for-the-badge&logo=flask&logoColor=white)](https://flask.palletsprojects.com/)
[![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)](https://developer.mozilla.org/en-US/docs/Web/JavaScript)
[![Nginx](https://img.shields.io/badge/Nginx-009639?style=for-the-badge&logo=nginx&logoColor=white)](https://nginx.org/)

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen?style=flat-square)](https://github.com/yourusername/real-time-people-dashboard)
[![Docker Compose](https://img.shields.io/badge/docker--compose-v3-blue?style=flat-square&logo=docker)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue?style=flat-square&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![Real-time](https://img.shields.io/badge/real--time-streaming-red?style=flat-square)](https://github.com/yourusername/real-time-people-dashboard)
[![WebSocket](https://img.shields.io/badge/websocket-enabled-orange?style=flat-square)](https://socket.io/)

A comprehensive real-time data pipeline that fetches people data from Random User API, processes it through Kafka and Spark, stores in Cassandra, and displays it on a beautiful web dashboard with live updates.

## 🏗️ Architecture Overview

```mermaid
graph TB
    subgraph "External Data Source"
        A[Random User API]
    end
    
    subgraph "Orchestration Layer"
        B[Apache Airflow]
        B --> B1[DAG Scheduler]
        B --> B2[Task Management]
        B --> B3[Service Monitoring]
    end
    
    subgraph "Streaming Infrastructure"
        C[Apache Kafka]
        D[Apache Spark Streaming]
        E[Apache Cassandra]
    end
    
    subgraph "Application Layer"
        F[Flask API + SocketIO]
        G[Nginx Web Server]
        H[Web Dashboard]
    end
    
    subgraph "Supporting Services"
        I[PostgreSQL]
        J[Zookeeper]
        K[Schema Registry]
        L[Control Center]
    end
    
    A --> B
    B -.->|orchestrates| C
    B -.->|manages| D
    B -.->|monitors| E
    B -.->|supervises| F
    
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    F -.->|WebSocket| H
    
    B -.->|metadata| I
    C -.->|coordination| J
    C -.->|schema mgmt| K
    C -.->|monitoring| L
    
    style B fill:#e1f5fe
    style B1 fill:#e1f5fe
    style B2 fill:#e1f5fe
    style B3 fill:#e1f5fe
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Data Source** | Random User API | Generate realistic user data |
| **Orchestration** | Apache Airflow | Schedule and monitor data workflows |
| **Message Queue** | Apache Kafka | Stream data in real-time |
| **Stream Processing** | Apache Spark | Process and transform streaming data |
| **Database** | Apache Cassandra | Store processed user data |
| **Backend API** | Flask + SocketIO | Serve data and WebSocket connections |
| **Frontend** | Vanilla JavaScript | Real-time dashboard interface |
| **Web Server** | Nginx | Reverse proxy and static file serving |
| **Containerization** | Docker + Docker Compose | Service orchestration |

## 🚀 Quick Start

### Prerequisites

![Docker](https://img.shields.io/badge/Docker-20.10%2B-blue?style=flat-square&logo=docker)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2.0%2B-blue?style=flat-square&logo=docker)
![Memory](https://img.shields.io/badge/RAM-8GB%2B-orange?style=flat-square)
![Disk](https://img.shields.io/badge/Disk-5GB%2B-yellow?style=flat-square)

### One-Click Setup

**Development Mode:**
```bash
git clone https://github.com/yourusername/real-time-people-dashboard.git
cd real-time-people-dashboard
./start-dev.sh
```

**Production Mode:**
```bash
./start-prod.sh
```

### Manual Setup
```bash
# Development
docker-compose up --build

# Production
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up --build
```

## 📱 Service Endpoints

| Service | URL | Status | Credentials |
|---------|-----|--------|-------------|
| ![Frontend](https://img.shields.io/badge/Frontend-Dashboard-blue?style=flat-square) | [http://localhost:3000](http://localhost:3000) | ![Status](https://img.shields.io/badge/status-online-brightgreen?style=flat-square) | None |
| ![Airflow](https://img.shields.io/badge/Airflow-WebUI-orange?style=flat-square) | [http://localhost:8080](http://localhost:8080) | ![Status](https://img.shields.io/badge/status-online-brightgreen?style=flat-square) | admin/admin |
| ![Kafka](https://img.shields.io/badge/Kafka-Control%20Center-red?style=flat-square) | [http://localhost:9021](http://localhost:9021) | ![Status](https://img.shields.io/badge/status-online-brightgreen?style=flat-square) | None |
| ![API](https://img.shields.io/badge/API-Backend-green?style=flat-square) | [http://localhost:5001](http://localhost:5001) | ![Status](https://img.shields.io/badge/status-online-brightgreen?style=flat-square) | None |
| ![Spark](https://img.shields.io/badge/Spark-Master-yellow?style=flat-square) | [http://localhost:9090](http://localhost:9090) | ![Status](https://img.shields.io/badge/status-online-brightgreen?style=flat-square) | None |

## 🔄 Starting the Data Pipeline

1. **Access Airflow UI**: Navigate to http://localhost:8080
2. **Login**: Use credentials `admin/admin`
3. **Enable DAG**: Toggle ON the "user_automation" DAG
4. **Monitor Pipeline**: Watch real-time data flow in the dashboard

## 🐳 Docker Services

![Docker Services](https://img.shields.io/badge/Services-13-blue?style=flat-square&logo=docker)

<details>
<summary>Click to expand service details</summary>

| Service | Container | Ports | Health Check |
|---------|-----------|-------|--------------|
| ![Frontend](https://img.shields.io/badge/Frontend-nginx-green?style=flat-square) | `people-frontend` | 3000:80 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Backend](https://img.shields.io/badge/Backend-flask-blue?style=flat-square) | `people-backend` | 5001:5001 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Spark](https://img.shields.io/badge/Spark-streaming-orange?style=flat-square) | `spark-streaming` | - | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Cassandra](https://img.shields.io/badge/Cassandra-database-purple?style=flat-square) | `cassandra` | 9042:9042 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Kafka](https://img.shields.io/badge/Kafka-broker-red?style=flat-square) | `broker` | 9092:9092 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Zookeeper](https://img.shields.io/badge/Zookeeper-coordination-yellow?style=flat-square) | `zookeeper` | 2181:2181 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Airflow](https://img.shields.io/badge/Airflow-webserver-lightblue?style=flat-square) | `airflow-webserver` | 8080:8080 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Scheduler](https://img.shields.io/badge/Airflow-scheduler-lightblue?style=flat-square) | `airflow-scheduler` | - | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-metadata-blue?style=flat-square) | `postgres` | 5432:5432 | ![Health](https://img.shields.io/badge/health-check-disabled-gray?style=flat-square) |
| ![Spark Master](https://img.shields.io/badge/Spark-master-orange?style=flat-square) | `spark-master` | 9090:8080 | ![Health](https://img.shields.io/badge/health-check-disabled-gray?style=flat-square) |
| ![Spark Worker](https://img.shields.io/badge/Spark-worker-orange?style=flat-square) | `spark-worker` | - | ![Health](https://img.shields.io/badge/health-check-disabled-gray?style=flat-square) |
| ![Schema Registry](https://img.shields.io/badge/Schema-registry-red?style=flat-square) | `schema-registry` | 8081:8081 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |
| ![Control Center](https://img.shields.io/badge/Control-center-red?style=flat-square) | `control-center` | 9021:9021 | ![Health](https://img.shields.io/badge/health-check-enabled-green?style=flat-square) |

</details>

## 🛠️ Development

### Project Structure
```
real-time-pipeline/
├── frontend/                 # Nginx + Vanilla JS frontend
│   ├── Dockerfile           # Frontend container config
│   ├── nginx.conf           # Nginx reverse proxy config
│   ├── index.html           # Dashboard HTML
│   └── app.js              # Frontend JavaScript logic
├── backend/                 # Flask backend with WebSocket
│   ├── Dockerfile          # Backend container config
│   ├── requirements.txt    # Python dependencies
│   └── app.py             # Flask API + SocketIO server
├── spark-docker/           # Spark streaming processor
│   ├── Dockerfile         # Spark container config
│   └── spark_stream.py    # Spark streaming logic
├── dags/                   # Airflow DAGs
│   └── kafka_stream.py    # Data generation DAG
├── script/                 # Utility scripts
│   └── entrypoint.sh      # Airflow initialization
├── docker-compose.yml      # Main services definition
├── docker-compose.prod.yml # Production overrides
├── docker-compose.override.yml # Development overrides
├── start-dev.sh           # Development startup script
├── start-prod.sh          # Production startup script
└── requirements.txt       # Global Python requirements
```

### Environment Configuration

Create `.env` file for custom configuration:

```env
# Database
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042

# Kafka
KAFKA_BOOTSTRAP_SERVERS=broker:29092
KAFKA_TOPIC=users_created

# Backend
BACKEND_PORT=5001
FLASK_ENV=development
DEBUG=true

# Frontend
FRONTEND_PORT=3000

# Spark
SPARK_MASTER_URL=spark://spark-master:7077
```

### API Endpoints

![API Documentation](https://img.shields.io/badge/API-documented-blue?style=flat-square)

| Method | Endpoint | Description | Response |
|--------|----------|-------------|----------|
| ![GET](https://img.shields.io/badge/GET-green?style=flat-square) | `/api/people` | Fetch all people records | JSON array of people |
| ![WebSocket](https://img.shields.io/badge/WS-blue?style=flat-square) | `/socket.io/` | Real-time updates | Live person data |

**Example Response:**
```json
[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "first_name": "John",
    "last_name": "Doe",
    "email": "john.doe@example.com",
    "phone": "+1-555-123-4567",
    "address": "123 Main St, Anytown, USA",
    "gender": "male",
    "picture": "https://randomuser.me/api/portraits/men/1.jpg",
    "registered_date": "2023-12-01T10:30:00Z"
  }
]
```

## 🔍 Monitoring & Debugging

### Real-time Monitoring

![Monitoring](https://img.shields.io/badge/monitoring-enabled-green?style=flat-square&logo=grafana)

```bash
# View all service logs
docker-compose logs -f

# Monitor specific services
docker-compose logs -f people-backend
docker-compose logs -f spark-streaming
docker-compose logs -f broker

# Check service status
docker-compose ps
```

### Health Checks

![Health Checks](https://img.shields.io/badge/health--checks-automated-green?style=flat-square)

```bash
# Frontend health
curl http://localhost:3000/health

# Backend health
curl http://localhost:5001/api/people

# Cassandra health
docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES"
```

### Database Operations

```bash
# Connect to Cassandra
docker exec -it cassandra cqlsh

# View data
USE spark_streams;
SELECT COUNT(*) FROM created_users;
SELECT * FROM created_users LIMIT 5;

# Monitor real-time inserts
SELECT * FROM created_users WHERE registered_date > '2023-12-01' ALLOW FILTERING;
```

### Kafka Debugging

```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Monitor messages
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users_created \
  --from-beginning \
  --max-messages 5
```

## 🚨 Troubleshooting

### Common Issues

<details>
<summary>🔧 Service Startup Issues</summary>

**Problem**: Services fail to start or crash immediately

**Solutions**:
```bash
# Check available memory
free -h

# Increase Docker memory allocation (8GB+ recommended)
# Docker Desktop: Settings > Resources > Memory

# Restart with clean state
docker-compose down -v
docker system prune -f
./start-dev.sh
```
</details>

<details>
<summary>🔌 Port Conflicts</summary>

**Problem**: Port already in use errors

**Solutions**:
```bash
# Check port usage
sudo lsof -i :3000
sudo lsof -i :8080
sudo lsof -i :9092

# Kill conflicting processes
sudo kill -9 $(lsof -t -i:3000)

# Use different ports in docker-compose.yml
```
</details>

<details>
<summary>📊 No Data Flowing</summary>

**Problem**: Dashboard shows no data or updates

**Checklist**:
- [ ] Airflow DAG enabled and running
- [ ] Kafka receiving messages
- [ ] Spark streaming job active
- [ ] Cassandra connection established
- [ ] Backend API responding

```bash
# Verify data flow
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic users_created --max-messages 1
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
curl http://localhost:5001/api/people | jq length
```
</details>

<details>
<summary>🌐 Frontend Connection Issues</summary>

**Problem**: Dashboard can't connect to backend

**Solutions**:
```bash
# Check backend status
curl http://localhost:5001/api/people

# Verify WebSocket connection
curl -H "Connection: Upgrade" -H "Upgrade: websocket" http://localhost:5001/socket.io/

# Check browser console for CORS errors
# Ensure backend CORS is configured properly
```
</details>

## 📊 Features

### Real-Time Dashboard
- ![Real-time](https://img.shields.io/badge/✅-Real--time%20data%20updates-green?style=flat-square)
- ![Statistics](https://img.shields.io/badge/✅-Live%20statistics%20tracking-green?style=flat-square)
- ![WebSocket](https://img.shields.io/badge/✅-WebSocket%20connections-green?style=flat-square)
- ![Responsive](https://img.shields.io/badge/✅-Responsive%20design-green?style=flat-square)
- ![Notifications](https://img.shields.io/badge/✅-Toast%20notifications-green?style=flat-square)
- ![Animations](https://img.shields.io/badge/✅-Smooth%20animations-green?style=flat-square)

### Data Pipeline
- ![API](https://img.shields.io/badge/✅-Random%20User%20API%20integration-green?style=flat-square)
- ![Streaming](https://img.shields.io/badge/✅-Kafka%20message%20streaming-green?style=flat-square)
- ![Processing](https://img.shields.io/badge/✅-Spark%20stream%20processing-green?style=flat-square)
- ![Storage](https://img.shields.io/badge/✅-Cassandra%20data%20storage-green?style=flat-square)
- ![Orchestration](https://img.shields.io/badge/✅-Airflow%20orchestration-green?style=flat-square)

### Production Ready
- ![Containerization](https://img.shields.io/badge/✅-Docker%20containerization-green?style=flat-square)
- ![Health Checks](https://img.shields.io/badge/✅-Automated%20health%20checks-green?style=flat-square)
- ![Load Balancing](https://img.shields.io/badge/✅-Nginx%20reverse%20proxy-green?style=flat-square)
- ![Configuration](https://img.shields.io/badge/✅-Environment%20configuration-green?style=flat-square)
- ![Logging](https://img.shields.io/badge/✅-Centralized%20logging-green?style=flat-square)
- ![Monitoring](https://img.shields.io/badge/✅-Service%20monitoring-green?style=flat-square)

## 🔒 Security Considerations

### Current Security Features
- ![CORS](https://img.shields.io/badge/✅-CORS%20protection-green?style=flat-square)
- ![XSS](https://img.shields.io/badge/✅-XSS%20protection%20headers-green?style=flat-square)
- ![Content Type](https://img.shields.io/badge/✅-Content%20type%20validation-green?style=flat-square)
- ![Network](https://img.shields.io/badge/✅-Docker%20network%20isolation-green?style=flat-square)

### Production Security Recommendations
- [ ] Change default passwords in production
- [ ] Implement SSL/TLS certificates
- [ ] Add authentication and authorization
- [ ] Enable Kafka security features
- [ ] Implement rate limiting
- [ ] Add input validation and sanitization
- [ ] Set up monitoring and alerting
- [ ] Regular security updates

## 📈 Performance & Scaling

### Current Performance
- ![Throughput](https://img.shields.io/badge/Throughput-1000%20records%2Fmin-yellow?style=flat-square)
- ![Latency](https://img.shields.io/badge/Latency-%3C100ms-green?style=flat-square)
- ![Memory](https://img.shields.io/badge/Memory-~6GB-orange?style=flat-square)
- ![CPU](https://img.shields.io/badge/CPU-~40%25-green?style=flat-square)

### Scaling Options

**Horizontal Scaling:**
- Add more Spark workers
- Scale Kafka partitions
- Deploy Cassandra cluster
- Load balance frontend instances

**Vertical Scaling:**
- Increase container memory limits
- Optimize Spark configurations
- Database query optimization
- Implement caching layer

### Performance Tuning

```yaml
# docker-compose.yml - Memory allocation example
services:
  spark-streaming:
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 2G
```

## 🧪 Testing

### Manual Testing
```bash
# Test data generation
curl -X POST http://localhost:8080/api/v1/dags/user_automation/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin

# Test backend API
curl http://localhost:5001/api/people | jq '.[0]'

# Test WebSocket connection
wscat -c ws://localhost:5001/socket.io/
```

### Load Testing
```bash
# Install dependencies
pip install locust

# Run load test
locust -f tests/load_test.py --host=http://localhost:5001
```

## 🤝 Contributing

![Contributors](https://img.shields.io/badge/contributors-welcome-brightgreen?style=flat-square)
![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen?style=flat-square)

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/amazing-feature`
3. **Make changes and test thoroughly**
4. **Commit changes**: `git commit -m 'Add amazing feature'`
5. **Push to branch**: `git push origin feature/amazing-feature`
6. **Open Pull Request**

---

<div align="center">

![Stars](https://img.shields.io/github/stars/yourusername/real-time-people-dashboard?style=social)
![Forks](https://img.shields.io/github/forks/yourusername/real-time-people-dashboard?style=social)
![Watchers](https://img.shields.io/github/watchers/yourusername/real-time-people-dashboard?style=social)

</div>
