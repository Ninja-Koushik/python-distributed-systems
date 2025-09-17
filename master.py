"""
Master.py
Description: This script acts as the master node in a distributed job queue system.
It uses gRPC to communicate with worker nodes, managing a job queue and handling job
dispatch and result collection. It now includes job persistence for fault tolerance.
"""

import grpc
from concurrent import futures
import time
import random
import json
import os
import sys

# Import the generated gRPC files for communication
import job_queue_pb2
import job_queue_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
# Filename for storing the persistent job queue
JOBS_FILE = "jobs.json"

class JobQueueServicer(job_queue_pb2_grpc.JobQueueServicer):
    """
    Implements the gRPC service for the master node.

    This class manages the lifecycle of jobs, handling requests from both
    clients (to submit jobs) and workers (to get and return jobs).
    """

    def __init__(self):
        """Initializes the master with a predefined job queue and empty data structures."""
        # The main job queue
        self.jobs = []
        # A dictionary to store the results of completed jobs
        self.job_results = {}
        # A dictionary to track currently connected workers
        self.workers = {}
        # Attempt to load any jobs that were persisted from a previous run
        self.load_jobs()

    def save_jobs(self):
        """Saves the current state of the job queue to a JSON file."""
        # Convert the protobuf messages to a list of dictionaries for JSON serialization
        jobs_data = [
            {"job_id": job.job_id, "job_type": job.job_type, "payload": job.payload}
            for job in self.jobs
        ]
        with open(JOBS_FILE, 'w') as f:
            json.dump(jobs_data, f, indent=4)
        print(f"Job queue saved to {JOBS_FILE}.")

    def load_jobs(self):
        """Loads the job queue from a JSON file if it exists, otherwise creates a new one."""
        if os.path.exists(JOBS_FILE):
            print(f"Loading jobs from {JOBS_FILE}...")
            with open(JOBS_FILE, 'r') as f:
                jobs_data = json.load(f)
            # Convert the dictionaries back into protobuf messages
            self.jobs = [
                job_queue_pb2.JobRequest(job_id=job["job_id"], job_type=job["job_type"], payload=job["payload"])
                for job in jobs_data
            ]
        else:
            print("No existing job file found. Creating a new job queue...")
            self.jobs = [
                job_queue_pb2.JobRequest(job_id=f"job_{i}", job_type="fibonacci", payload=random.randint(20, 40))
                for i in range(100)
            ]
            self.save_jobs() # Persist the initial queue

    def GetJobs(self, request, context):
        """
        [Streaming RPC] Provides a stream of jobs to a connected worker.
        """
        worker_id = request.worker_id
        self.workers[worker_id] = True
        print(f"Worker {worker_id} connected and requesting jobs.")

        while self.jobs:
            # We pop the first job to send it, and save the remaining queue state
            job = self.jobs.pop(0)
            self.save_jobs()
            yield job
            time.sleep(0.5)

        print(f"Master has no more jobs for worker {worker_id}. Disconnecting.")
        self.workers.pop(worker_id, None)

    def SendResult(self, request, context):
        """
        [RPC] Receives the result of a completed job from a worker.
        """
        print(f"Received result for job {request.job_id} from worker {request.worker_id}: {request.result}")
        self.job_results[request.job_id] = request.result
        return job_queue_pb2.Ack(success=True)

    def SendJob(self, request, context):
        """
        [RPC] Receives a new job from a client and adds it to the queue.
        """
        self.jobs.append(request)
        self.save_jobs() # Save the new job to the file
        print(f"New job {request.job_id} added to the queue.")
        return job_queue_pb2.JobReply(message=f"Job {request.job_id} received.")

def serve():
    """Starts the gRPC server and begins serving RPC calls."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    job_queue_pb2_grpc.add_JobQueueServicer_to_server(
        JobQueueServicer(), server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Master server started on port 50051.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
