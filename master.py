"""
Master.py
Description: This script acts as the master node in a distributed job queue system.
It uses gRPC to communicate with worker nodes, managing a job queue and handling job
dispatch and result collection. It includes job persistence and worker health checks
for enhanced fault tolerance.
"""

import grpc
from concurrent import futures
import time
import random
import json
import os
import sys
import threading
import queue

# Import the generated gRPC files for communication
import job_queue_pb2
import job_queue_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
JOBS_FILE = "jobs.json"
HEARTBEAT_TIMEOUT = 10  # Time in seconds before a worker is considered dead

class JobQueueServicer(job_queue_pb2_grpc.JobQueueServicer):
    """
    Implements the gRPC service for the master node.

    This class manages the lifecycle of jobs, handling requests from both
    clients (to submit jobs) and workers (to get and return jobs).
    """

    def __init__(self):
        """Initializes the master with a predefined job queue and empty data structures."""
        # Use a queue for thread-safe job management
        self.jobs = queue.Queue()
        self.job_results = {}
        self.workers_last_heartbeat = {}
        self.jobs_in_progress = {} # To track jobs currently being processed
        self.lock = threading.Lock()
        self.load_jobs()

    def save_jobs(self):
        """Saves the current state of the job queue and jobs in progress to a JSON file."""
        with self.lock:
            # Get all jobs from the queue and put them back
            jobs_list = []
            while not self.jobs.empty():
                jobs_list.append(self.jobs.get())
            for job in jobs_list:
                self.jobs.put(job)
            
            all_jobs_to_save = jobs_list + list(self.jobs_in_progress.values())

            jobs_data = [
                {"job_id": job.job_id, "job_type": job.job_type, "payload": job.payload}
                for job in all_jobs_to_save
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
            for job_data in jobs_data:
                job = job_queue_pb2.JobRequest(
                    job_id=job_data["job_id"], 
                    job_type=job_data["job_type"], 
                    payload=job_data["payload"]
                )
                self.jobs.put(job)
        else:
            print("No existing job file found. Creating a new job queue...")
            for i in range(100):
                job = job_queue_pb2.JobRequest(
                    job_id=f"job_{i}", 
                    job_type="fibonacci", 
                    payload=random.randint(20, 40)
                )
                self.jobs.put(job)
            self.save_jobs()

    def Heartbeat(self, request, context):
        """[RPC] Receives a heartbeat from a worker."""
        worker_id = request.worker_id
        with self.lock:
            self.workers_last_heartbeat[worker_id] = time.time()
        # print(f"Received heartbeat from worker {worker_id}")
        return job_queue_pb2.Ack(success=True)

    def GetJobs(self, request, context):
        """
        [Streaming RPC] Provides a stream of jobs to a connected worker.
        """
        worker_id = request.worker_id
        with self.lock:
            self.workers_last_heartbeat[worker_id] = time.time()
        print(f"Worker {worker_id} connected and requesting jobs.")

        while True:
            try:
                # Blocks until a job is available
                job = self.jobs.get(timeout=1)
                
                with self.lock:
                    self.jobs_in_progress[job.job_id] = job
                
                yield job
                time.sleep(0.5)
            except queue.Empty:
                if not self.jobs_in_progress:
                    print(f"Master has no more jobs for worker {worker_id}.")
                    break
                # If the queue is empty, wait for a new job or a dead worker check
                time.sleep(1)

        with self.lock:
            self.workers_last_heartbeat.pop(worker_id, None)

    def SendResult(self, request, context):
        """
        [RPC] Receives the result of a completed job from a worker.
        """
        print(f"Received result for job {request.job_id} from worker {request.worker_id}: {request.result}")
        with self.lock:
            self.job_results[request.job_id] = request.result
            self.jobs_in_progress.pop(request.job_id, None)
        self.save_jobs()
        return job_queue_pb2.Ack(success=True)

    def SendJob(self, request, context):
        """
        [RPC] Receives a new job from a client and adds it to the queue.
        """
        self.jobs.put(request)
        self.save_jobs()
        print(f"New job {request.job_id} added to the queue.")
        return job_queue_pb2.JobReply(message=f"Job {request.job_id} received.")

    def check_dead_workers(self):
        """A background method to check for workers that haven't sent a heartbeat recently."""
        while True:
            dead_workers = []
            with self.lock:
                # Re-queue jobs from workers who have timed out
                for worker_id, last_heartbeat in self.workers_last_heartbeat.items():
                    if time.time() - last_heartbeat > HEARTBEAT_TIMEOUT:
                        print(f"Worker {worker_id} timed out. Re-queuing its jobs.")
                        dead_workers.append(worker_id)
            
            for worker_id in dead_workers:
                with self.lock:
                    self.workers_last_heartbeat.pop(worker_id, None)
                
                jobs_to_requeue = [
                    job for job in self.jobs_in_progress.values() 
                    if job.job_id not in self.job_results # Check if result was received
                ]
                
                for job in jobs_to_requeue:
                    print(f"Re-queuing job {job.job_id}.")
                    self.jobs.put(job)
                    with self.lock:
                        self.jobs_in_progress.pop(job.job_id)
                self.save_jobs()
            
            time.sleep(5) # Check every 5 seconds

def serve():
    """Starts the gRPC server and begins serving RPC calls."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = JobQueueServicer()
    job_queue_pb2_grpc.add_JobQueueServicer_to_server(
        servicer, server
    )
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Master server started on port 50051.")
    
    # Start the background thread for checking worker health
    heartbeat_checker_thread = threading.Thread(target=servicer.check_dead_workers, daemon=True)
    heartbeat_checker_thread.start()
    
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
