"""
Master.py

Description:
This script acts as the master node in a distributed job queue system. This
version features enhanced fault tolerance and state management.
- It tracks job states (PENDING, DISPATCHED, COMPLETED, FAILED) for greater insight.
- It includes a new RPC endpoint to allow clients to poll for job status.
- The worker health check logic is more precise, correctly re-queuing only
  jobs assigned to failed workers.
"""

import sys
import os
import json
import time
import random
import logging
import threading
import queue
from typing import Dict, Any, List

import grpc
from concurrent import futures

# Import the generated gRPC files
import job_queue_pb2
import job_queue_pb2_grpc

# --- Configuration ---
_ONE_DAY_IN_SECONDS = 60 * 60 * 24
JOBS_FILE = "jobs_state.json"
HEARTBEAT_TIMEOUT = 10  # Seconds before a worker is considered unresponsive
REQUEUE_CHECK_INTERVAL = 5 # Seconds between health checks

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

class JobState:
    """A simple class to hold the state of a job."""
    PENDING = job_queue_pb2.JobStatusReply.PENDING
    DISPATCHED = job_queue_pb2.JobStatusReply.DISPATCHED
    COMPLETED = job_queue_pb2.JobStatusReply.COMPLETED
    FAILED = job_queue_pb2.JobStatusReply.FAILED

class JobQueueServicer(job_queue_pb2_grpc.JobQueueServicer):
    """
    Implements the gRPC service for the master node.

    This servicer manages the entire job queue lifecycle, from accepting new jobs
    to dispatching them to workers, tracking their status, and handling failures.
    """

    def __init__(self):
        """Initializes the master with a predefined job queue and state management."""
        self.pending_jobs: queue.Queue = queue.Queue()
        self.jobs_in_progress: Dict[str, job_queue_pb2.JobRequest] = {}
        # job_states tracks every job, including its status, worker, and result
        self.job_states: Dict[str, Dict[str, Any]] = {}
        # workers_last_heartbeat tracks when each worker last sent a heartbeat
        self.workers_last_heartbeat: Dict[str, float] = {}
        # worker_job_map tracks which job each worker is currently handling
        self.worker_job_map: Dict[str, str] = {}
        self.lock = threading.RLock()  # A reentrant lock to prevent deadlocks in concurrent access
        
        self.is_running = True
        self.load_jobs()

    def _save_jobs(self) -> None:
        """
        Saves the current state of all jobs to a JSON file.

        This method is called after any state change to ensure the master can
        recover its state in case of a crash.
        """
        with self.lock:
            # Consolidate all jobs into a single list to save
            all_jobs_to_save: List[Dict] = []
            for job_id, state_info in self.job_states.items():
                job_data = {
                    "job_id": job_id,
                    "job_type": state_info["job"].job_type,
                    "payload": state_info["job"].payload,
                    "status": state_info["status"],
                    "result": state_info.get("result", ""),
                    "worker": state_info.get("worker", "")
                }
                all_jobs_to_save.append(job_data)
            
            try:
                with open(JOBS_FILE, 'w') as f:
                    json.dump(all_jobs_to_save, f, indent=4)
                logging.info(f"Job queue state saved to '{JOBS_FILE}'.")
            except IOError as e:
                logging.error(f"Failed to save job queue to file: {e}")

    def load_jobs(self) -> None:
        """
        Loads jobs and their states from a JSON file.

        If the file exists, it will restore the state of the job queue.
        If not, it will initialize a new queue with sample jobs.
        """
        if os.path.exists(JOBS_FILE):
            logging.info(f"Attempting to load jobs from '{JOBS_FILE}'...")
            try:
                with open(JOBS_FILE, 'r') as f:
                    jobs_data = json.load(f)
                
                with self.lock:
                    for job_data in jobs_data:
                        job = job_queue_pb2.JobRequest(
                            job_id=job_data["job_id"],
                            job_type=job_data["job_type"],
                            payload=job_data["payload"]
                        )
                        self.job_states[job.job_id] = {
                            "job": job,
                            "status": job_data["status"],
                            "result": job_data.get("result", ""),
                            "worker": job_data.get("worker", "")
                        }
                        # Put pending jobs back in the queue
                        if job_data["status"] == JobState.PENDING:
                            self.pending_jobs.put(job)
                        # Re-add dispatched jobs to the in_progress map
                        elif job_data["status"] == JobState.DISPATCHED:
                            self.jobs_in_progress[job.job_id] = job
                            self.worker_job_map[job_data["worker"]] = job.job_id
                logging.info(f"Loaded {len(jobs_data)} jobs from file.")
            except (IOError, json.JSONDecodeError) as e:
                logging.error(f"Error loading jobs from file: {e}. Initializing a new queue.")
                self._initialize_sample_jobs()
        else:
            logging.info("No existing job file found. Initializing with sample jobs...")
            self._initialize_sample_jobs()

    def _initialize_sample_jobs(self) -> None:
        """Initializes the job queue with a set of sample jobs."""
        for i in range(100):
            job = job_queue_pb2.JobRequest(
                job_id=f"job_{i}",
                job_type="fibonacci",
                payload=random.randint(20, 40)
            )
            self.pending_jobs.put(job)
            self.job_states[job.job_id] = {"job": job, "status": JobState.PENDING}
        self._save_jobs()

    def Heartbeat(self, request, context) -> job_queue_pb2.Ack:
        """
        [RPC] Receives a heartbeat from a worker, updating its last active time.
        
        Args:
            request (job_queue_pb2.WorkerInfo): The worker's ID.
            context (grpc.ServicerContext): The context for the RPC.

        Returns:
            job_queue_pb2.Ack: A success acknowledgment.
        """
        worker_id = request.worker_id
        with self.lock:
            self.workers_last_heartbeat[worker_id] = time.time()
        return job_queue_pb2.Ack(success=True)

    def GetJobs(self, request, context) -> job_queue_pb2.JobRequest:
        """
        [Streaming RPC] Provides a continuous stream of jobs to a connected worker.

        The master pulls from its pending job queue and yields jobs to the worker,
        marking them as "DISPATCHED".

        Args:
            request (job_queue_pb2.WorkerInfo): The worker's ID.
            context (grpc.ServicerContext): The context for the RPC.

        Yields:
            job_queue_pb2.JobRequest: A job from the pending queue.
        """
        worker_id = request.worker_id
        with self.lock:
            self.workers_last_heartbeat[worker_id] = time.time()
        logging.info(f"Worker '{worker_id}' connected and requesting jobs.")

        while context.is_active() and self.is_running:
            try:
                # Blocks until a job is available or 1 second passes
                job = self.pending_jobs.get(timeout=1)
                
                with self.lock:
                    self.jobs_in_progress[job.job_id] = job
                    self.worker_job_map[worker_id] = job.job_id
                    self.job_states[job.job_id]["status"] = JobState.DISPATCHED
                    self.job_states[job.job_id]["worker"] = worker_id
                
                self._save_jobs()
                yield job
            except queue.Empty:
                pass
        
        logging.info(f"Worker '{worker_id}' disconnected. Releasing associated resources.")
        with self.lock:
            self.workers_last_heartbeat.pop(worker_id, None)

    def SendResult(self, request, context) -> job_queue_pb2.Ack:
        """
        [RPC] Receives the result of a completed job from a worker.

        Args:
            request (job_queue_pb2.JobResult): The job result, worker ID, and success status.
            context (grpc.ServicerContext): The context for the RPC.

        Returns:
            job_queue_pb2.Ack: A success acknowledgment.
        """
        logging.info(f"Received result for job '{request.job_id}' from worker '{request.worker_id}'.")
        with self.lock:
            job_state_info = self.job_states.get(request.job_id)
            if job_state_info:
                job_state_info["result"] = request.result
                job_state_info["status"] = JobState.COMPLETED if request.success else JobState.FAILED
                
                # Remove the job from the in-progress map and worker's assignment
                self.jobs_in_progress.pop(request.job_id, None)
                self.worker_job_map.pop(request.worker_id, None)

        self._save_jobs()
        return job_queue_pb2.Ack(success=True)

    def SendJob(self, request, context) -> job_queue_pb2.JobReply:
        """
        [RPC] Receives a new job from a client and adds it to the queue.

        Args:
            request (job_queue_pb2.JobRequest): The job to be added.
            context (grpc.ServicerContext): The context for the RPC.

        Returns:
            job_queue_pb2.JobReply: A reply confirming receipt of the job.
        """
        with self.lock:
            if request.job_id in self.job_states:
                logging.warning(f"Job ID '{request.job_id}' already exists. Ignoring.")
                return job_queue_pb2.JobReply(message=f"Job {request.job_id} already exists.")
            
            self.pending_jobs.put(request)
            self.job_states[request.job_id] = {"job": request, "status": JobState.PENDING}

        self._save_jobs()
        logging.info(f"New job '{request.job_id}' added to the queue.")
        return job_queue_pb2.JobReply(message=f"Job {request.job_id} received.")

    def GetJobStatus(self, request, context) -> job_queue_pb2.JobStatusReply:
        """
        [RPC] Returns the current status of a specific job.

        Args:
            request (job_queue_pb2.JobStatusRequest): The job ID to check.
            context (grpc.ServicerContext): The context for the RPC.

        Returns:
            job_queue_pb2.JobStatusReply: The job's status and final result if available.
        """
        with self.lock:
            job_state_info = self.job_states.get(request.job_id)
            if not job_state_info:
                return job_queue_pb2.JobStatusReply(
                    status=job_queue_pb2.JobStatusReply.UNKNOWN,
                    status_message="Job ID not found."
                )
            
            status = job_state_info.get("status", JobState.PENDING)
            result = job_state_info.get("result", "")
            
            status_map = {
                JobState.PENDING: "PENDING",
                JobState.DISPATCHED: "DISPATCHED",
                JobState.COMPLETED: "COMPLETED",
                JobState.FAILED: "FAILED"
            }
            status_message = status_map.get(status, "UNKNOWN")

            return job_queue_pb2.JobStatusReply(
                status=status,
                status_message=status_message,
                result=result
            )

    def check_dead_workers_loop(self) -> None:
        """
        A background thread method to periodically check for unresponsive workers.

        Workers that have not sent a heartbeat within HEARTBEAT_TIMEOUT are
        considered dead. Their assigned jobs are then re-queued for another
        worker to pick up.
        """
        while self.is_running:
            dead_worker_ids = []
            with self.lock:
                current_time = time.time()
                # Find workers that have not heartbeaten recently
                for worker_id, last_heartbeat in list(self.workers_last_heartbeat.items()):
                    if (current_time - last_heartbeat) > HEARTBEAT_TIMEOUT:
                        dead_worker_ids.append(worker_id)
            
            for worker_id in dead_worker_ids:
                logging.warning(f"Worker '{worker_id}' timed out. Re-queuing its jobs.")
                with self.lock:
                    self.workers_last_heartbeat.pop(worker_id, None)
                    job_id = self.worker_job_map.pop(worker_id, None)
                    
                    if job_id and self.job_states.get(job_id):
                        job = self.jobs_in_progress.pop(job_id, None)
                        if job:
                            logging.info(f"Re-queuing job '{job_id}'.")
                            self.pending_jobs.put(job)
                            self.job_states[job_id]["status"] = JobState.PENDING
                            self.job_states[job_id].pop("worker", None)
                            self._save_jobs()
            
            time.sleep(REQUEUE_CHECK_INTERVAL)

def serve():
    """Starts the gRPC server and begins serving RPC calls."""
    try:
        # Create a gRPC server with a thread pool to handle concurrent requests
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        servicer = JobQueueServicer()
        job_queue_pb2_grpc.add_JobQueueServicer_to_server(servicer, server)
        server.add_insecure_port('[::]:50051')
        server.start()
        logging.info("Master server started on port 50051.")

        # Start the background thread for checking dead workers
        heartbeat_checker_thread = threading.Thread(
            target=servicer.check_dead_workers_loop,
            daemon=True,
            name="HeartbeatChecker"
        )
        heartbeat_checker_thread.start()

        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            logging.info("Shutting down server gracefully...")
            servicer.is_running = False
            server.stop(0)
    except Exception as e:
        logging.critical(f"Server failed to start: {e}")
        sys.exit(1)

if __name__ == '__main__':
    serve()
