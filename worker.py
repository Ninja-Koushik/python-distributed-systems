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
    """
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a

def run_worker(worker_id):
    """
    Connects to the master, requests jobs, processes them, and sends the results back.
    """
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = job_queue_pb2_grpc.JobQueueStub(channel)

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
            # This block handles the case where the master server is disconnected or crashed
            print(f"Worker {worker_id} disconnected: {e}. The master may have crashed or shut down.")

if __name__ == '__main__':
    worker_id = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
    run_worker(worker_id)
