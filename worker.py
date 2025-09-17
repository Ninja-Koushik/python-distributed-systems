"""
Worker.py
Description: This script acts as a worker node in a distributed job queue system.
It connects to a master node via gRPC to receive, process, and return the results
of jobs. It includes heartbeat functionality for improved fault tolerance.
"""

import grpc
import sys
import uuid
import threading
import time

# Import the generated gRPC files for communication
import job_queue_pb2
import job_queue_pb2_grpc

def fibonacci(n):
    """
    A simple function to simulate a long-running task by calculating the nth Fibonacci number.

    Args:
        n (int): The number for which to calculate the Fibonacci value.

    Returns:
        int: The nth Fibonacci number.
    """
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a

def send_heartbeats(stub, worker_id):
    """A background thread to send periodic heartbeats to the master."""
    heartbeat_interval = 5  # seconds
    while True:
        try:
            stub.Heartbeat(job_queue_pb2.WorkerInfo(worker_id=worker_id))
        except grpc.RpcError as e:
            print(f"Heartbeat failed: {e}. Master may be down. Exiting.")
            sys.exit(1)
        time.sleep(heartbeat_interval)

def run_worker(worker_id):
    """
    Connects to the master, requests jobs, processes them, and sends the results back.

    Args:
        worker_id (str): A unique ID for this worker instance.
    """
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = job_queue_pb2_grpc.JobQueueStub(channel)

        # Start the heartbeat thread
        heartbeat_thread = threading.Thread(target=send_heartbeats, args=(stub, worker_id), daemon=True)
        heartbeat_thread.start()

        try:
            jobs_stream = stub.GetJobs(job_queue_pb2.WorkerInfo(worker_id=worker_id))
            print(f"Worker {worker_id} connected to master, requesting jobs...")

            for job in jobs_stream:
                print(f"Worker {worker_id} processing job {job.job_id} (Type: {job.job_type}, Payload: {job.payload})...")
                
                result = None
                if job.job_type == "fibonacci":
                    result = fibonacci(job.payload)
                else:
                    result = "Unknown job type"
                
                print(f"Job {job.job_id} completed with result: {result}")
                
                response = stub.SendResult(job_queue_pb2.JobResult(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    result=str(result)
                ))
                if response.success:
                    print(f"Result for job {job.job_id} sent successfully.")
                
        except grpc.RpcError as e:
            print(f"Worker {worker_id} disconnected: {e}. Master may have crashed or shut down.")
            # The heartbeat thread will catch this and exit the process.

if __name__ == '__main__':
    worker_id = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
    run_worker(worker_id)
