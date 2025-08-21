#!/bin/bash

echo "🚀 Starting Real-Time People Dashboard (Production Mode)"
echo "=================================================="

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker-compose -f docker-compose.yml -f docker-compose.prod.yml down

# Build and start all services
echo "🔨 Building and starting services..."
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d

# Wait for services to start
echo "⏳ Waiting for services to start..."
sleep 15

# Check service health
echo "🏥 Checking service health..."
docker-compose -f docker-compose.yml -f docker-compose.prod.yml ps

echo ""
echo "✅ Production application started successfully!"
echo ""
echo "📱 Frontend Dashboard: http://localhost:3000"
echo "🔧 Airflow UI: http://localhost:8080 (admin/admin)"
echo "📊 Confluent Control Center: http://localhost:9021"
echo ""
echo "💡 To view logs: docker-compose -f docker-compose.yml -f docker-compose.prod.yml logs -f [service-name]"
echo "🛑 To stop: docker-compose -f docker-compose.yml -f docker-compose.prod.yml down"