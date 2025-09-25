"""
masterServer.py

Description:
This script implements the Master node in a distributed job queue system using gRPC.
It serves as the central point of control, responsible for:

1.  Maintaining a job queue for incoming tasks.
2.  Dispatching jobs to available worker nodes via gRPC streaming.
3.  Monitoring worker health using a dedicated heartbeat mechanism.
4.  Re-queuing jobs from workers that have timed out or failed.
5.  Storing and updating the status and results of all submitted jobs.
6.  Responding to client requests (from the dashboard) for submitting new jobs
    and retrieving the status of all jobs.

This design is highly fault-tolerant, with a background thread dedicated to
monitoring worker health. If a worker fails, its in-progress job is
automatically returned to the queue for another worker to pick up.
"""

import grpc
from concurrent import futures
import time
import json
import threading
import logging
from typing import Dict, Any, List

# Import the generated gRPC files for communication with clients and workers
# Assumes these files are generated from job_queue.proto and are in the same directory.
import job_queue_pb2
import job_queue_pb2_grpc

# --- Configuration ---
MASTER_PORT = 50051             # The port on which the Master gRPC server will run.
WORKER_TIMEOUT = 10             # Time in seconds before a worker is considered failed.
MAX_WORKERS = 10                # Maximum number of concurrent worker threads the gRPC server can handle.

# Configure logging to provide clear, timestamped feedback on server activity.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

# --- Job and Worker State Definitions (for internal use) ---
# These constant strings are used to maintain a consistent state machine for jobs.
STATUS_QUEUED = "QUEUED"
STATUS_DISPATCHED = "DISPATCHED"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"
STATUS_UNKNOWN = "UNKNOWN"

class JobQueueService(job_queue_pb2_grpc.JobQueueServicer):
    """
    Implements the gRPC JobQueue service methods as defined in job_queue.proto.

    This class manages all the internal state of the distributed system, including
    the job queue, worker heartbeats, and the status of every job.
    """

    def __init__(self):
        # The main job queue (FIFO - First-In, First-Out).
        self.job_queue: List[job_queue_pb2.JobRequest] = []
        # A lock to ensure thread-safe access to the job queue and other shared state.
        self.job_queue_lock = threading.Lock()
        
        # A persistent dictionary storing the state of every job, indexed by job_id.
        self.all_jobs: Dict[str, Dict[str, Any]] = {}

        # A dictionary to track the last heartbeat time for each active worker.
        # Format: {worker_id: timestamp}
        self.worker_heartbeats: Dict[str, float] = {}

        # A map of jobs currently being processed by workers.
        # Format: {job_id: worker_id}
        self.jobs_in_progress: Dict[str, str] = {}
        
        # A Condition Variable to wake up workers waiting for a new job.
        # This is more efficient than a constant polling loop.
        self.job_available_condition = threading.Condition(self.job_queue_lock)

        # Start a background thread to continuously monitor worker heartbeats.
        threading.Thread(target=self._monitor_workers, daemon=True, name="WorkerMonitorThread").start()
        logging.info("Worker monitor thread started.")

    # ----------------------------------------------------------------------
    # gRPC Service Methods
    # ----------------------------------------------------------------------

    def SendJob(self, request: job_queue_pb2.JobRequest, context) -> job_queue_pb2.Ack:
        """
        RPC method to add a new job to the queue.

        This method is called by the Dashboard Server to submit a new task.

        Args:
            request (job_queue_pb2.JobRequest): The job to be added.
            context: gRPC context.

        Returns:
            job_queue_pb2.Ack: An acknowledgement indicating success or failure.
        """
        with self.job_queue_lock:
            # Add the new job to the end of the queue.
            self.job_queue.append(request)
            
            # Record the job's initial state in the persistent job store.
            self.all_jobs[request.job_id] = {
                "job_id": request.job_id,
                "job_type": request.job_type,
                "payload": request.payload,
                "status": STATUS_QUEUED,
                "result": None,
            }
            logging.info(f"New job '{request.job_id}' of type '{request.job_type}' queued.")
            
            # Notify a waiting worker that a new job is available.
            self.job_available_condition.notify()
            
            return job_queue_pb2.Ack(success=True, message=f"Job '{request.job_id}' queued.")

    def Heartbeat(self, request: job_queue_pb2.WorkerInfo, context) -> job_queue_pb2.Empty:
        """
        RPC method to receive a heartbeat from a worker.

        This is a non-blocking RPC. It simply updates the worker's last seen time.

        Args:
            request (job_queue_pb2.WorkerInfo): The worker's ID.
            context: gRPC context.

        Returns:
            job_queue_pb2.Empty: An empty response.
        """
        # Update the timestamp for the worker's last heartbeat.
        self.worker_heartbeats[request.worker_id] = time.time()
        # logging.debug(f"Heartbeat received from worker '{request.worker_id}'.")
        return job_queue_pb2.Empty()

    def GetJobs(self, request: job_queue_pb2.WorkerInfo, context):
        """
        Server streaming RPC to dispatch jobs to a worker.

        A worker calls this method and the Master keeps the connection open,
        streaming jobs as they become available. This is more efficient than
        polling.

        Args:
            request (job_queue_pb2.WorkerInfo): The connecting worker's ID.
            context: gRPC context.

        Yields:
            job_queue_pb2.JobRequest: The next available job from the queue.
        """
        worker_id = request.worker_id
        logging.info(f"Worker '{worker_id}' connected and ready to receive jobs.")
        # The initial connection serves as the first heartbeat.
        self.worker_heartbeats[worker_id] = time.time()

        while context.is_active():
            job_request = None
            with self.job_queue_lock:
                while not self.job_queue:
                    # If the queue is empty, the worker thread waits here until a job is added.
                    self.job_available_condition.wait()
                
                # Dequeue the first job from the queue.
                job_request = self.job_queue.pop(0)
                
                # Update the job's status to DISPATCHED and track which worker has it.
                self.jobs_in_progress[job_request.job_id] = worker_id
                self.all_jobs[job_request.job_id]["status"] = STATUS_DISPATCHED

            # Yield the job to the worker. This sends the job and pauses the
            # server stream until the worker is ready for the next job.
            yield job_request

    def SendResult(self, request: job_queue_pb2.JobResult, context) -> job_queue_pb2.Ack:
        """
        RPC method to receive the result of a completed or failed job from a worker.

        Args:
            request (job_queue_pb2.JobResult): The result of the executed job.
            context: gRPC context.

        Returns:
            job_queue_pb2.Ack: An acknowledgement.
        """
        job_id = request.job_id
        
        with self.job_queue_lock:
            # Check if the job exists.
            if job_id not in self.all_jobs:
                return job_queue_pb2.Ack(success=False, message=f"Job ID '{job_id}' not found.")
            
            # Remove the job from the in-progress list.
            if job_id in self.jobs_in_progress:
                del self.jobs_in_progress[job_id]

            # Update the job's status and result based on the worker's report.
            status = STATUS_COMPLETED if request.success else STATUS_FAILED
            self.all_jobs[job_id]["status"] = status
            self.all_jobs[job_id]["result"] = request.result

            logging.info(f"Job '{job_id}' finished by worker '{request.worker_id}'. Status: {status}")

            return job_queue_pb2.Ack(success=True, message="Result recorded.")

    def GetAllJobs(self, request: job_queue_pb2.Empty, context) -> job_queue_pb2.AllJobsResponse:
        """
        RPC method to return the status of all jobs.

        This method is called by the Dashboard Server to get data for the UI.

        Args:
            request (job_queue_pb2.Empty): An empty request.
            context: gRPC context.

        Returns:
            job_queue_pb2.AllJobsResponse: A message containing a JSON string
                                          of all job statuses.
        """
        with self.job_queue_lock:
            # Serialize the dictionary of all jobs to a JSON string.
            jobs_json = json.dumps(self.all_jobs)
            return job_queue_pb2.AllJobsResponse(jobs_json=jobs_json)

    # ----------------------------------------------------------------------
    # Internal Monitoring Thread
    # ----------------------------------------------------------------------

    def _monitor_workers(self):
        """
        A background thread function that periodically checks for timed-out workers.

        This is a critical component for fault tolerance. If a worker fails to
        send a heartbeat, any job it was processing is considered lost and is
        re-queued for another worker.
        """
        while True:
            # Sleep for half the timeout period to check workers frequently.
            time.sleep(WORKER_TIMEOUT / 2)
            
            dead_workers: List[str] = []
            
            # Iterate through heartbeats to find workers that have timed out.
            for worker_id, last_time in list(self.worker_heartbeats.items()):
                if time.time() - last_time > WORKER_TIMEOUT:
                    dead_workers.append(worker_id)
            
            if not dead_workers:
                continue

            # Lock the shared state to safely handle dead workers and their jobs.
            with self.job_queue_lock:
                re_queued_count = 0
                for worker_id in dead_workers:
                    # Remove the timed-out worker from the heartbeats map.
                    del self.worker_heartbeats[worker_id]
                    logging.warning(f"Worker '{worker_id}' timed out. Assuming failure.")

                    # Find and re-queue any jobs this worker was processing.
                    jobs_to_re_queue = [
                        job_id for job_id, wid in self.jobs_in_progress.items() 
                        if wid == worker_id
                    ]
                    
                    for job_id in jobs_to_re_queue:
                        # Re-construct the original job request from the stored data.
                        job_data = self.all_jobs[job_id]
                        re_queue_request = job_queue_pb2.JobRequest(
                            job_id=job_id,
                            job_type=job_data["job_type"],
                            payload=job_data["payload"]
                        )
                        
                        # Add the job back to the front of the queue for priority.
                        self.job_queue.insert(0, re_queue_request)
                        # Reset the job's status to QUEUED.
                        self.all_jobs[job_id]["status"] = STATUS_QUEUED
                        # Remove the job from the in-progress map.
                        del self.jobs_in_progress[job_id]
                        re_queued_count += 1

                if re_queued_count > 0:
                    logging.warning(f"Re-queued {re_queued_count} job(s) from failed workers.")
                    # Wake up any workers that might be waiting for jobs.
                    self.job_available_condition.notify_all()


def serve():
    """
    Main function to start and run the gRPC server.
    """
    # Create a gRPC server with a thread pool to handle concurrent requests.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MAX_WORKERS))
    # Register the JobQueue service implementation with the server.
    job_queue_pb2_grpc.add_JobQueueServicer_to_server(JobQueueService(), server)
    # Bind the server to a network address and port.
    server.add_insecure_port(f'[::]:{MASTER_PORT}')
    
    logging.info(f"Master Server started and listening on port {MASTER_PORT}")
    server.start()
    
    # Keep the main thread alive to prevent the server from exiting immediately.
    try:
        while True:
            time.sleep(86400)  # Sleep for one day.
    except KeyboardInterrupt:
        logging.info("Master Server shutting down...")
        server.stop(0)
        
if __name__ == '__main__':
    serve()
