"""
Worker.py
Description: This script acts as a worker node in a distributed job queue system.
It connects to a master node via gRPC to receive, process, and return the results
of jobs.
"""

import grpc
import sys
import uuid

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

def run_worker(worker_id):
    """
    Connects to the master, requests jobs, processes them, and sends the results back.

    Args:
        worker_id (str): A unique ID for this worker instance.
    """
    # Create an insecure channel to connect to the master
    with grpc.insecure_channel('localhost:50051') as channel:
        # Create a stub (client) to call the master's services
        stub = job_queue_pb2_grpc.JobQueueStub(channel)

        try:
            # Use a streaming RPC to get a continuous flow of jobs from the master
            jobs_stream = stub.GetJobs(job_queue_pb2.WorkerInfo(worker_id=worker_id))
            print(f"Worker {worker_id} connected to master, requesting jobs...")

            # Iterate over the jobs provided by the master
            for job in jobs_stream:
                print(f"Worker {worker_id} processing job {job.job_id} (Type: {job.job_type}, Payload: {job.payload})...")
                
                result = None
                # Execute the job based on its type
                if job.job_type == "fibonacci":
                    result = fibonacci(job.payload)
                else:
                    result = "Unknown job type"
                
                print(f"Job {job.job_id} completed with result: {result}")
                
                # Send the result back to the master using a dedicated RPC
                response = stub.SendResult(job_queue_pb2.JobResult(
                    job_id=job.job_id,
                    worker_id=worker_id,
                    result=str(result) # Convert result to string for the protobuf message
                ))
                if response.success:
                    print(f"Result for job {job.job_id} sent successfully.")
                
        except grpc.RpcError as e:
            # Handle disconnection from the master
            print(f"Worker {worker_id} disconnected: {e}")

if __name__ == '__main__':
    # Use a command-line argument for the worker ID or generate a random one
    worker_id = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
    run_worker(worker_id)
