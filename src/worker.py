"""
Worker.py

Description:
This script acts as a worker node in a distributed job queue system. It connects to a
master node via gRPC to receive, process, and return job results. The worker
continuously polls the master for new tasks and sends periodic heartbeats to signal
that it is alive and available. This design ensures that the master can detect
unresponsive workers and re-queue their jobs for a new worker to handle.
"""

import sys
import uuid
import threading
import time
import logging
from typing import NoReturn
import signal

import grpc

# Import the generated gRPC files
import job_queue_pb2
import job_queue_pb2_grpc

# --- Configuration ---
MASTER_HOST = 'localhost'
MASTER_PORT = 50051
HEARTBEAT_INTERVAL = 5  # seconds
SHUTDOWN_TIMEOUT = 5    # seconds to wait for graceful shutdown

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
)

def fibonacci(n: int) -> int:
    """
    Calculates the nth Fibonacci number. This function simulates a compute-bound task.

    Args:
        n (int): The number for which to calculate the Fibonacci value.

    Returns:
        int: The nth Fibonacci number.
    """
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b

def _process_job(job: job_queue_pb2.JobRequest) -> str:
    """
    Processes a received job based on its type.

    Args:
        job (job_queue_pb2.JobRequest): The job to be processed.

    Returns:
        str: The result of the job as a string.
    """
    logging.info(
        f"Processing job '{job.job_id}' (Type: '{job.job_type}', Payload: '{job.payload}')..."
    )
    
    result = None
    if job.job_type == "fibonacci":
        try:
            result = fibonacci(job.payload)
        except RecursionError as e:
            logging.error(f"Fibonacci calculation for payload '{job.payload}' failed: {e}")
            result = "Error: Recursion depth exceeded"
        except Exception as e:
            logging.error(f"An unexpected error occurred during job processing: {e}")
            result = "Error: An unexpected error occurred"
    else:
        logging.warning(f"Unknown job type '{job.job_type}' for job '{job.job_id}'.")
        result = "Error: Unknown job type"
        
    logging.info(f"Job '{job.job_id}' completed with result: '{result}'.")
    return str(result)

def _send_heartbeats(stub: job_queue_pb2_grpc.JobQueueStub, worker_id: str, stop_event: threading.Event) -> None:
    """
    A background thread to send periodic heartbeats to the master.

    Args:
        stub (job_queue_pb2_grpc.JobQueueStub): The gRPC stub for communication.
        worker_id (str): The unique ID for this worker.
        stop_event (threading.Event): An event to signal the thread to stop.
    """
    while not stop_event.is_set():
        try:
            stub.Heartbeat(job_queue_pb2.WorkerInfo(worker_id=worker_id))
            # Wait for the next heartbeat interval or until the stop event is set.
            stop_event.wait(HEARTBEAT_INTERVAL)
        except grpc.RpcError as e:
            logging.error(f"Heartbeat failed. Master may be down. Details: {e.details()}")
            stop_event.set()  # Signal other threads to stop
            sys.exit(1)
        except Exception as e:
            logging.critical(f"An unexpected error occurred in heartbeat thread: {e}")
            stop_event.set()

def run_worker(worker_id: str) -> None:
    """
    Establishes a connection to the master, starts the heartbeat thread,
    and enters a loop to fetch and process jobs.

    Args:
        worker_id (str): A unique ID for this worker instance.
    """
    target_address = f"{MASTER_HOST}:{MASTER_PORT}"
    logging.info(f"Worker '{worker_id}' starting and connecting to master at {target_address}...")
    
    stop_event = threading.Event()
    
    # Register a signal handler for graceful shutdown
    def signal_handler(signum, frame):
        logging.info("Shutdown signal received. Exiting gracefully...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        with grpc.insecure_channel(target_address) as channel:
            stub = job_queue_pb2_grpc.JobQueueStub(channel)

            # Start the heartbeat thread. It's a daemon so it won't prevent the main process from exiting.
            heartbeat_thread = threading.Thread(
                target=_send_heartbeats,
                args=(stub, worker_id, stop_event),
                daemon=True,
                name="HeartbeatThread"
            )
            heartbeat_thread.start()

            # The main loop for fetching and processing jobs
            while not stop_event.is_set():
                try:
                    jobs_stream = stub.GetJobs(job_queue_pb2.WorkerInfo(worker_id=worker_id))
                    for job in jobs_stream:
                        if stop_event.is_set():
                            break
                        
                        result = _process_job(job)
                        
                        response = stub.SendResult(job_queue_pb2.JobResult(
                            job_id=job.job_id,
                            worker_id=worker_id,
                            result=str(result)
                        ))
                        if not response.success:
                            logging.warning(f"Master failed to acknowledge result for job '{job.job_id}'.")
                    
                    # If the stream ends, it could be a graceful shutdown from the master.
                    if not stop_event.is_set():
                        logging.info("Master disconnected the job stream. Attempting to reconnect...")
                        time.sleep(5)  # Wait before retrying
                
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        logging.error(f"Could not connect to master. Is it running? Details: {e.details()}")
                    else:
                        logging.error(f"RPC error during job stream: {e.details()}")
                    
                    logging.info("Connection lost. Waiting for master to become available...")
                    stop_event.wait(5)
                except Exception as e:
                    logging.critical(f"An unexpected error occurred in the main worker loop: {e}")
                    stop_event.set()

    except grpc.FutureTimeoutError:
        logging.error("Connection to master timed out.")
        stop_event.set()
    except Exception as e:
        logging.critical(f"Worker startup failed: {e}")
        stop_event.set()
    finally:
        logging.info("Worker shutting down...")
        stop_event.set()

if __name__ == '__main__':
    # Get worker ID from command line or generate a unique one
    worker_id = sys.argv[1] if len(sys.argv) > 1 else str(uuid.uuid4())
    run_worker(worker_id)
