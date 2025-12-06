# =================================================================
# 🚀 Telecom Fraud Detection System - Windows Startup Script
# =================================================================

Write-Host "Starting Telecom Fraud Detection System..." -ForegroundColor Green
Write-Host "-----------------------------------------------------"

# 1. Start Infrastructure (Docker)
Write-Host "`n[1/5] Starting Infrastructure (Docker)..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker Compose failed to start." -ForegroundColor Red
    exit 1
}
Write-Host "✅ Infrastructure is up." -ForegroundColor Green

# 2. Wait for Kafka & Initialize Topics
Write-Host "`n[2/5] Waiting for Kafka & Initializing Topics..." -ForegroundColor Yellow
Write-Host "   (This may take a minute if Kafka is restarting)"
python src/source/init_kafka.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to initialize Kafka." -ForegroundColor Red
    exit 1
}

# 3. Initialize ClickHouse Database
Write-Host "`n[3/5] Initializing ClickHouse Database..." -ForegroundColor Yellow
python src/source/init_clickhouse.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to initialize ClickHouse." -ForegroundColor Red
    exit 1
}

# 4. Start Producer
Write-Host "`n[4/5] Starting Data Producer..." -ForegroundColor Yellow
Start-Process python -ArgumentList "src/source/producer.py" -RedirectStandardOutput "logs/producer.log" -RedirectStandardError "logs/producer_error.log" -WindowStyle Minimized
Write-Host "✅ Producer started. Logs: logs/producer.log" -ForegroundColor Green

# 5. Instructions for Spark (must run in WSL)
Write-Host "`n[5/5] Ready to start Spark!" -ForegroundColor Yellow
Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "IMPORTANT: Spark must run in WSL" -ForegroundColor Cyan  
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Open a WSL terminal and run:" -ForegroundColor White
Write-Host "  cd /mnt/d/ITI-Data_Engineer/Projects/Telecom-Fraud-Detection-Pipeline" -ForegroundColor Yellow
Write-Host "  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/processing/stream_processor.py" -ForegroundColor Yellow
Write-Host ""
Write-Host "Monitor progress:" -ForegroundColor White
Write-Host "  - Producer logs: logs/producer.log" -ForegroundColor Gray
Write-Host "  - Grafana dashboard: http://localhost:3000" -ForegroundColor Gray
Write-Host ""
