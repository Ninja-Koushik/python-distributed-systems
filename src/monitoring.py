"""
Description: Implements a Prometheus metrics server for Ninja Nucleus to collect and expose job and Worker metrics.
- Collects job status counts (submitted, running, completed, failed) and Worker metrics (active count, heartbeat latency).
- Exposes /metrics endpoint (port 8001) for Prometheus scraping.
- Provides /update endpoint (port 8000) for Master and Workers to send metrics via HTTP POST.
- Designed for small-scale batch processing, task automation, and prototyping applications.
- Integrates with masterServer.py (job status updates) and worker.py (heartbeats).

Usage:
    Run `python monitoring.py` to start the metrics server.
    Configure Master and Workers to send metrics to http://localhost:8000/update.
    Configure Prometheus to scrape http://localhost:8001/metrics.
"""

from prometheus_client import start_http_server, Counter, Gauge, Histogram
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import logging

# Configure logging for debugging and monitoring server activity
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Prometheus metrics definitions
# Counter for job statuses (increment-only, e.g., submitted, running, completed, failed)
job_status_counter = Counter(
    'ninja_nucleus_jobs_total',
    'Total number of jobs processed by status',
    ['status']
)

# Gauge for number of active Workers (can increase or decrease based on heartbeats)
worker_active_gauge = Gauge(
    'ninja_nucleus_workers_active',
    'Number of active Workers in the system'
)

# Histogram for Worker heartbeat latency (tracks distribution of response times)
heartbeat_latency_histogram = Histogram(
    'ninja_nucleus_heartbeat_latency_seconds',
    'Latency of Worker heartbeats in seconds',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0]
)

# In-memory store for metrics data (simulates Master/Worker communication)
# Structure: {'jobs': {'submitted': int, 'running': int, 'completed': int, 'failed': int},
#            'workers': {worker_id: last_heartbeat_timestamp}}
metrics_data = {
    'jobs': {'submitted': 0, 'running': 0, 'completed': 0, 'failed': 0},
    'workers': {}
}

class MetricsHandler(BaseHTTPRequestHandler):
    """
    HTTP handler for /update endpoint to receive metrics from Master and Workers.
    Expects POST requests with JSON payload:
    - {'job_status': 'status'} for job updates (e.g., 'completed').
    - {'worker_id': 'id', 'heartbeat': timestamp} for Worker heartbeats.
    """
    def do_POST(self):
        """Handle POST requests to update metrics."""
        try:
            # Read and parse JSON payload
            content_length = int(self.headers['Content-Length'])
            post_data = json.loads(self.rfile.read(content_length))
            logger.info(f"Received metrics update: {post_data}")

            # Update job metrics
            if 'job_status' in post_data:
                status = post_data['job_status']
                if status in metrics_data['jobs']:
                    metrics_data['jobs'][status] += 1
                    job_status_counter.labels(status=status).inc()
                    logger.info(f"Incremented job status counter: {status}")

            # Update Worker metrics
            if 'worker_id' in post_data and 'heartbeat' in post_data:
                worker_id = post_data['worker_id']
                timestamp = post_data['heartbeat']
                metrics_data['workers'][worker_id] = timestamp
                worker_active_gauge.set(len(metrics_data['workers']))
                latency = time.time() - timestamp
                heartbeat_latency_histogram.observe(latency)
                logger.info(f"Updated Worker {worker_id} heartbeat, active count: {len(metrics_data['workers'])}")

            # Send success response
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
        except Exception as e:
            logger.error(f"Error processing metrics update: {e}")
            self.send_response(400)
            self.end_headers()
            self.wfile.write(f"Error: {str(e)}".encode())

def start_metrics_server():
    """Start HTTP server to receive metrics updates on port 8000."""
    server = HTTPServer(('localhost', 8000), MetricsHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    logger.info("Metrics update server started at http://localhost:8000")

def cleanup_old_workers():
    """Periodically remove inactive Workers (no heartbeat for >10s) to keep active count accurate."""
    while True:
        try:
            current_time = time.time()
            inactive_workers = [
                worker_id for worker_id, timestamp in metrics_data['workers'].items()
                if current_time - timestamp > 10
            ]
            for worker_id in inactive_workers:
                del metrics_data['workers'][worker_id]
                logger.info(f"Removed inactive Worker: {worker_id}")
            worker_active_gauge.set(len(metrics_data['workers']))
            time.sleep(5)  # Check every 5 seconds
        except Exception as e:
            logger.error(f"Error cleaning up workers: {e}")

def main():
    """Start Prometheus metrics server and metrics update server."""
    # Start Prometheus metrics server on port 8001
    start_http_server(8001)
    logger.info("Prometheus metrics server started at http://localhost:8001/metrics")

    # Start metrics update server on port 8000
    start_metrics_server()

    # Start background thread to clean up inactive Workers
    cleanup_thread = threading.Thread(target=cleanup_old_workers)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down monitoring server")

if __name__ == '__main__':
    main()

```
