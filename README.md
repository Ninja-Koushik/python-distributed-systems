# Ninja Nucleus

Ninja Nucleus is a fault-tolerant, distributed job queue system built with gRPC and Python. It enables efficient task distribution across worker nodes, real-time job monitoring via a web dashboard, and robust handling of worker failures. The system is designed for scalability and reliability, with features like job persistence, worker health monitoring, and a user-friendly interface for job submission and tracking.

## Features

- **Distributed Task Processing**: Submit jobs to a master node, which dispatches them to available workers via gRPC streaming.
- **Fault Tolerance**: Automatically re-queues jobs from failed workers using a heartbeat mechanism.
- **Real-Time Monitoring**: A Flask-based web dashboard displays live job statuses and allows job submission.
- **Job Persistence**: Job states are maintained in memory (with optional JSON file persistence in older versions).
- **Extensible Job Types**: Supports custom job types (e.g., Fibonacci computation) with a modular worker design.

## Architecture

Ninja Nucleus consists of three main components:

1. **Master Node** (`masterServer.py`):
   - Manages the job queue and dispatches tasks to workers.
   - Tracks job states (`QUEUED`, `DISPATCHED`, `COMPLETED`, `FAILED`).
   - Monitors worker health via heartbeats and re-queues jobs from timed-out workers.
2. **Worker Nodes** (`worker.py`):
   - Fetch jobs from the master via gRPC streaming.
   - Execute tasks (e.g., compute Fibonacci numbers) and report results.
   - Send periodic heartbeats to signal availability.
3. **Dashboard** (`dashboardServer.py`, `dashboard.html`):
   - A Flask web server serving a UI to submit jobs and view real-time statuses.
   - Proxies gRPC calls to the master for job submission and status queries.

The system uses protocol buffers (`job_queue.proto`) for gRPC communication, defining messages like `JobRequest`, `JobResult`, and RPCs like `SendJob` and `GetAllJobs`.

## Prerequisites

- **Python**: 3.8 or higher
- **Dependencies**:
  - `grpcio` and `grpcio-tools` for gRPC communication
  - `flask` for the dashboard server
- **Protobuf Compiler**: For generating Python gRPC files from `job_queue.proto`

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/ninja-nucleus.git
   cd ninja-nucleus
   ```

2. **Install Dependencies**:
   Create a virtual environment and install required packages:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install grpcio grpcio-tools flask
   ```

3. **Generate gRPC Files**:
   Compile the protocol buffer file to generate Python stubs:
   ```bash
   python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. job_queue.proto
   ```
   This generates `job_queue_pb2.py` and `job_queue_pb2_grpc.py`.

## Usage

1. **Start the Master Node**:
   Run the master server to manage jobs and workers:
   ```bash
   python masterServer.py
   ```
   The master listens on `localhost:50051` by default.

2. **Start Worker Nodes**:
   Run one or more worker nodes to process jobs:
   ```bash
   python worker.py
   ```
   Each worker connects to the master, sends heartbeats, and processes jobs (e.g., Fibonacci calculations).

3. **Start the Dashboard**:
   Launch the Flask dashboard server to monitor and submit jobs:
   ```bash
   python dashboardServer.py
   ```
   Access the dashboard at `http://localhost:5001`.

4. **Submit and Monitor Jobs**:
   - Open the dashboard in a browser.
   - Submit a job by entering a `job_type` (e.g., `fibonacci`) and `payload` (e.g., an integer for Fibonacci).
   - View real-time job statuses in the dashboard table.

## Example Job File

The repository includes `jobs.json`, a sample file with Fibonacci jobs. The master can process these jobs if loaded (older versions used file-based persistence). Example entry:
```json
{
    "job_id": "job_31",
    "job_type": "fibonacci",
    "payload": 30
}
```

## Configuration

- **Master Node** (`masterServer.py`):
  - `MASTER_PORT`: 50051 (gRPC server port)
  - `WORKER_TIMEOUT`: 10 seconds (time before a worker is considered failed)
  - `MAX_WORKERS`: 10 (max concurrent worker threads)
- **Worker Node** (`worker.py`):
  - `MASTER_HOST`: `localhost`
  - `MASTER_PORT`: 50051
  - `HEARTBEAT_INTERVAL`: 3 seconds
  - `CONNECTION_RETRY_INTERVAL`: 5 seconds
- **Dashboard** (`dashboardServer.py`):
  - Runs on port 5001
  - Connects to master at `localhost:50051`

Modify these in the respective scripts if needed.

## Extending the System

- **Add New Job Types**:
  1. Update `worker.py` to include new handlers in `JobExecutor.job_handlers`:
     ```python
     self.job_handlers["new_job_type"] = new_job_function
     ```
  2. Ensure the master and dashboard support the new `job_type`.
- **Persistent Storage**:
  - The current master uses in-memory storage. To persist jobs to disk (as in older versions), implement file I/O in `masterServer.py` similar to `master.py`'s `_save_jobs` and `load_jobs`.
- **Scaling**:
  - Run multiple workers on different machines by updating `MASTER_HOST`.
  - Use a load balancer or multiple master instances for high availability (requires additional synchronization).

## Testing

To validate the system:
1. Start the master (`python masterServer.py`).
2. Start one or more workers (`python worker.py`).
3. Start the dashboard (`python dashboardServer.py`).
4. Open `http://localhost:5001`, submit a Fibonacci job (e.g., payload 10), and verify the result appears in the dashboard.
5. Simulate a worker failure by stopping a worker process; the master should re-queue its job within 10 seconds.

## Troubleshooting

- **gRPC Errors**: Ensure `job_queue_pb2.py` and `job_queue_pb2_grpc.py` are generated and match `job_queue.proto`.
- **Dashboard Issues**: Verify `dashboard.html` is in the same directory as `dashboardServer.py`.
- **Worker Timeouts**: Check `HEARTBEAT_INTERVAL` and `WORKER_TIMEOUT` alignment.
- **Port Conflicts**: Ensure ports 50051 (master) and 5001 (dashboard) are free.

## Contributing

Contributions are welcome! Please:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Commit changes (`git commit -m "Add your feature"`).
4. Push to the branch (`git push origin feature/your-feature`).
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.