"""
Master.py
Description: This script acts as the master node in a distributed job queue system.
It uses gRPC to communicate with worker nodes, managing a job queue and handling job
dispatch and result collection.
"""

import grpc
from concurrent import futures
import time
import random
import sys

# Import the generated gRPC files for communication
import job_queue_pb2
import job_queue_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

class JobQueueServicer(job_queue_pb2_grpc.JobQueueServicer):
    """
    Implements the gRPC service for the master node.

    This class manages the lifecycle of jobs, handling requests from both
    clients (to submit jobs) and workers (to get and return jobs).
    """

    def __init__(self):
        """Initializes the master with a predefined job queue and empty data structures."""
        # A list to store jobs that are waiting to be processed
        self.jobs = [
            job_queue_pb2.JobRequest(job_id=f"job_{i}", job_type="fibonacci", payload=random.randint(20, 40))
            for i in range(100)
        ]
        # A dictionary to store the results of completed jobs
        self.job_results = {}
        # A dictionary to track currently connected workers
        self.workers = {}

    def GetJobs(self, request, context):
        """
        [Streaming RPC] Provides a stream of jobs to a connected worker.

        Args:
            request: The WorkerInfo message from the worker.
            context: The gRPC context for the RPC call.

        Yields:
            A JobRequest message for each job in the queue.
        """
        worker_id = request.worker_id
        # Add the worker to our tracking dictionary
        self.workers[worker_id] = True
        print(f"Worker {worker_id} connected and requesting jobs.")

        while self.jobs:
            # Pop the first job from the queue and send it to the worker
            job = self.jobs.pop(0)
            yield job
            # Small delay to simulate a real-world system
            time.sleep(0.5)

        print(f"Master has no more jobs for worker {worker_id}. Disconnecting.")
        # Optional: Remove worker from tracking once the queue is empty
        self.workers.pop(worker_id, None)

    def SendResult(self, request, context):
        """
        [RPC] Receives the result of a completed job from a worker.

        Args:
            request: The JobResult message containing the job's ID, worker ID, and result.
            context: The gRPC context.

        Returns:
            An Ack message indicating the result was received successfully.
        """
        print(f"Received result for job {request.job_id} from worker {request.worker_id}: {request.result}")
        # Store the result using the job's ID as the key
        self.job_results[request.job_id] = request.result
        return job_queue_pb2.Ack(success=True)

    def SendJob(self, request, context):
        """
        [RPC] Receives a new job from a client and adds it to the queue.

        Args:
            request: The JobRequest message from the client.
            context: The gRPC context.

        Returns:
            A JobReply message confirming the job was received.
        """
        self.jobs.append(request)
        print(f"New job {request.job_id} added to the queue.")
        return job_queue_pb2.JobReply(message=f"Job {request.job_id} received.")

def serve():
    """Starts the gRPC server and begins serving RPC calls."""
    # Create a gRPC server with a thread pool executor
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Register the JobQueueServicer with the server
    job_queue_pb2_grpc.add_JobQueueServicer_to_server(
        JobQueueServicer(), server
    )
    # Bind the server to a specific port
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Master server started on port 50051.")
    # Wait for the server to terminate
    server.wait_for_termination()

if __name__ == '__main__':
    # Run the main server function when the script is executed
    serve()
