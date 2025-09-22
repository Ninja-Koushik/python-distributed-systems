"""
Worker.py

Description:
This script acts as a worker node in a distributed job queue system. Its primary
functions are to:
1. Continuously request jobs from the master node.
2. Send periodic heartbeats to the master to signal that it is alive.
3. Execute the received jobs based on their type.
4. Report the job's result back to the master.

This version is more fault-tolerant, with a dedicated heartbeat thread and robust
error handling for network failures.
"""

import grpc
import sys
import uuid
import logging
import time
import threading
from typing import Dict, Any, Callable

# Import the generated gRPC files for communication with the master
import job_queue_pb2
import job_queue_pb2_grpc

# --- Configuration ---
MASTER_HOST = 'localhost'
MASTER_PORT = 50051
HEARTBEAT_INTERVAL = 3  # Time in seconds between heartbeats
CONNECTION_RETRY_INTERVAL = 5 # Time to wait before attempting to reconnect

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

def fibonacci(n: int) -> int:
    """
    Computes the nth Fibonacci number.

    This is an example of a CPU-bound task that a worker would execute.
    
    Args:
        n (int): The integer payload.
    
    Returns:
        int: The nth Fibonacci number.
    """
    if n <= 1:
        return n
    
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b

class JobExecutor:
    """
    Manages the lifecycle of a worker node.

    This class handles the core logic of connecting to the master, sending
    heartbeats, fetching and processing jobs, and reporting results.
    """

    def __init__(self, master_address: str):
        """
        Initializes the JobExecutor with a unique worker ID and a gRPC channel.

        Args:
            master_address (str): The address of the master node (e.g., 'localhost:50051').
        """
        self.worker_id = f"worker_{uuid.uuid4()}"
        self.master_address = master_address
        self.channel = None
        self.stub = None
        self.is_running = True
        
        # A dictionary mapping job types to their corresponding execution functions
        self.job_handlers: Dict[str, Callable] = {
            "fibonacci": fibonacci,
            # Add more job types and handlers here
        }

    def _send_heartbeat(self) -> None:
        """
        A dedicated thread method to send heartbeats to the master.

        This function runs in a separate daemon thread to ensure the master
        knows the worker is still active, even if it is busy processing a job.
        """
        while self.is_running:
            try:
                # Use a new channel for each heartbeat to ensure connection validity
                with grpc.insecure_channel(self.master_address) as channel:
                    stub = job_queue_pb2_grpc.JobQueueStub(channel)
                    stub.Heartbeat(job_queue_pb2.WorkerInfo(worker_id=self.worker_id))
                logging.debug(f"Heartbeat sent by '{self.worker_id}'.")
            except grpc.RpcError as e:
                logging.warning(
                    f"Heartbeat failed: {e.details()}. Master may be temporarily unavailable."
                )
            except Exception as e:
                logging.error(f"An unexpected error occurred in heartbeat loop: {e}")
            
            time.sleep(HEARTBEAT_INTERVAL)

    def _process_job(self, job: job_queue_pb2.JobRequest) -> job_queue_pb2.JobResult:
        """
        Executes a received job and returns the result.

        Args:
            job (job_queue_pb2.JobRequest): The job to be executed.
        
        Returns:
            job_queue_pb2.JobResult: The result of the job execution, including
                                      the success status and the output.
        """
        logging.info(
            f"Processing job '{job.job_id}' of type '{job.job_type}' with payload: {job.payload}"
        )
        result = ""
        success = False
        try:
            handler = self.job_handlers.get(job.job_type)
            if handler:
                result = str(handler(job.payload))
                success = True
                logging.info(f"Job '{job.job_id}' completed successfully. Result: {result}")
            else:
                result = "Unknown job type."
                logging.error(f"Job '{job.job_id}' failed: Unknown job type '{job.job_type}'.")
        except Exception as e:
            result = f"Error processing job: {e}"
            logging.error(f"Job '{job.job_id}' failed with an exception: {e}")
        finally:
            return job_queue_pb2.JobResult(
                job_id=job.job_id,
                worker_id=self.worker_id,
                result=result,
                success=success
            )

    def run(self) -> None:
        """
        The main method for the worker.

        This method connects to the master and enters a loop to continuously
        fetch, execute, and report on jobs.
        """
        logging.info(f"Starting worker '{self.worker_id}'.")

        # Start the background heartbeat thread
        heartbeat_thread = threading.Thread(
            target=self._send_heartbeat,
            daemon=True,
            name="HeartbeatThread"
        )
        heartbeat_thread.start()

        while self.is_running:
            try:
                # Use a context manager for the channel to ensure it closes properly
                with grpc.insecure_channel(self.master_address) as channel:
                    self.stub = job_queue_pb2_grpc.JobQueueStub(channel)
                    logging.info("Connected to master. Requesting jobs...")
                    
                    # The GetJobs RPC streams jobs from the master
                    for job in self.stub.GetJobs(job_queue_pb2.WorkerInfo(worker_id=self.worker_id)):
                        job_result = self._process_job(job)
                        self.stub.SendResult(job_result)
                        
            except grpc.RpcError as e:
                logging.error(
                    f"Connection to master lost: {e.details()}. Retrying in {CONNECTION_RETRY_INTERVAL} seconds..."
                )
                time.sleep(CONNECTION_RETRY_INTERVAL)
            except KeyboardInterrupt:
                logging.info("Shutting down worker gracefully...")
                self.is_running = False
            except Exception as e:
                logging.critical(f"An unexpected error occurred: {e}. Shutting down.")
                self.is_running = False
        
        logging.info("Worker process has stopped.")

if __name__ == '__main__':
    try:
        executor = JobExecutor(f"{MASTER_HOST}:{MASTER_PORT}")
        executor.run()
    except Exception as e:
        logging.critical(f"Failed to initialize worker: {e}")
        sys.exit(1)
