"""
dashboardServer.py

This script runs a simple Flask web server to serve the dashboard UI and act
as a proxy for gRPC calls to the Master node. It is designed to be run in a
separate process from the Master and Workers.

Endpoints:
- GET /: Serves the dashboard.html file.
- POST /submit_job: Receives job data from the web form and submits it as a gRPC
  request to the Master.
- GET /get_jobs: Fetches the current status of all jobs from the Master via gRPC
  and returns the data as JSON for the dashboard to display.
"""

import sys
import uuid
import json
import logging
from flask import Flask, request, jsonify, send_file
from typing import Dict, Any

import grpc
import job_queue_pb2
import job_queue_pb2_grpc

# --- Configuration ---
# The address of the gRPC master node.
MASTER_HOST = 'localhost'
MASTER_PORT = 50051

# Configure logging to provide clear, timestamped feedback.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

# Initialize the Flask application.
app = Flask(__name__)

@app.route('/')
def serve_dashboard():
    """
    Serves the main dashboard HTML file to the client.
    """
    # The dashboard.html file must be in the same directory as this script.
    return send_file('dashboard.html')

@app.route('/submit_job', methods=['POST')
def submit_job():
    """
    Handles a POST request to submit a new job.

    It retrieves the job details from the form data, creates a gRPC job request,
    and sends it to the Master.

    Returns:
        A JSON response indicating whether the submission was successful or not.
    """
    job_type = request.form.get('job_type')
    payload = request.form.get('payload')
    
    if not job_type or not payload:
        return jsonify({"success": False, "message": "Job type and payload are required."}), 400

    job_id = f"job_{uuid.uuid4()}"
    
    try:
        # The gRPC proto expects an integer payload, so we must convert it.
        payload_int = int(payload)
    except ValueError:
        return jsonify({"success": False, "message": "Payload must be a valid integer."}), 400

    try:
        # Establish an insecure gRPC channel to the Master.
        with grpc.insecure_channel(f'{MASTER_HOST}:{MASTER_PORT}') as channel:
            stub = job_queue_pb2_grpc.JobQueueStub(channel)
            job = job_queue_pb2.JobRequest(
                job_id=job_id,
                job_type=job_type,
                payload=payload_int
            )
            # Send the job to the Master using the SendJob RPC.
            response = stub.SendJob(job)
            logging.info(f"Job '{job_id}' submitted via dashboard.")
            return jsonify({
                "success": response.success,
                "message": response.message,
                "job_id": job_id
            })
    except grpc.RpcError as e:
        # Catch gRPC errors (e.g., if the Master is down) and report them.
        logging.error(f"Failed to submit job to Master: {e.details()}")
        return jsonify({
            "success": False,
            "message": f"Connection to master failed: {e.details()}"
        }), 500

@app.route('/get_jobs')
def get_jobs():
    """
    Handles a GET request to retrieve all current job statuses.

    It acts as a proxy for the dashboard's polling mechanism, calling the new
    GetAllJobs RPC on the Master.

    Returns:
        A JSON object containing all jobs and their states.
    """
    try:
        # Connect to the gRPC master.
        with grpc.insecure_channel(f'{MASTER_HOST}:{MASTER_PORT}') as channel:
            stub = job_queue_pb2_grpc.JobQueueStub(channel)
            # Call the GetAllJobs RPC to get a JSON string of all jobs.
            all_jobs_response = stub.GetAllJobs(job_queue_pb2.Empty())
            
            # Deserialize the JSON string returned by the Master.
            jobs = json.loads(all_jobs_response.jobs_json)
            
            return jsonify(jobs)
    except grpc.RpcError as e:
        # Return an error message if the connection to the Master fails.
        logging.error(f"Failed to fetch job statuses from Master: {e.details()}")
        return jsonify({
            "error": "Failed to connect to master."
        }), 500

if __name__ == '__main__':
    # Run the Flask app on port 5000. This server will be the front door for the dashboard.
    app.run(port=5000, debug=False)
