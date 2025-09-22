"""
Client.py

Description:
This script acts as a client for a distributed job queue system. It connects to
a master node via gRPC, generates a unique job, and submits it for processing.

The script expects two command-line arguments:
1.  <job_type>: The category of the job (e.g., "fibonacci", "prime_check").
2.  <payload>: The integer input data required for the job's execution.

Dependencies:
- grpcio: The core gRPC library for Python.
- job_queue_pb2: Generated gRPC message definitions.
- job_queue_pb2_grpc: Generated gRPC service definitions.
"""
import sys
import uuid
import logging
import argparse
from typing import NoReturn

import grpc

# Import the generated gRPC files
import job_queue_pb2
import job_queue_pb2_grpc

# Configure logging for a cleaner output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def _parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments and validates the input.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.
    
    Raises:
        SystemExit: If arguments are missing or the payload is not a valid integer.
    """
    parser = argparse.ArgumentParser(
        description="Submit a new job to the distributed job queue master."
    )
    parser.add_argument(
        "job_type",
        type=str,
        help="The type of job to submit (e.g., 'fibonacci')."
    )
    parser.add_argument(
        "payload",
        type=int,
        help="The integer data for the job."
    )
    
    args = parser.parse_args()
    return args

def submit_job(job_type: str, payload: int, host: str = 'localhost', port: int = 50051) -> None:
    """
    Creates and sends a single job request to the master node.

    This function establishes a gRPC channel and uses a stub to call the 'SendJob'
    RPC method, submitting the job to the queue.

    Args:
        job_type (str): The type of job (e.g., "fibonacci").
        payload (int): The integer input data for the job.
        host (str): The hostname or IP of the master node.
        port (int): The port number of the master node.
    
    Returns:
        None: The function doesn't return a value but logs the outcome of the RPC call.

    Raises:
        grpc.RpcError: If the client fails to connect to the master or the RPC call
                       encounters an error.
    """
    job_id = f"client_job_{uuid.uuid4()}"
    target_address = f"{host}:{port}"
    
    logging.info(f"Connecting to master at {target_address}...")
    
    try:
        # Create an insecure channel to connect to the master.
        # This is suitable for development; secure channels should be used in production.
        with grpc.insecure_channel(target_address) as channel:
            # Create a stub (client) to call the master's services.
            stub = job_queue_pb2_grpc.JobQueueStub(channel)

            # Create the JobRequest message with the provided data.
            job_request = job_queue_pb2.JobRequest(
                job_id=job_id,
                job_type=job_type,
                payload=payload
            )
            
            logging.info(f"Submitting job: ID={job_id}, Type={job_type}, Payload={payload}")
            
            # Call the SendJob RPC method on the master.
            response = stub.SendJob(job_request)
            
            logging.info(f"Master's response: {response.message}")
            
    except grpc.RpcError as e:
        # Catch gRPC-specific errors, such as a connection failure.
        logging.error(f"Could not connect to or communicate with the master: {e.details()}")
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            logging.warning("Please ensure the master server is running and accessible.")
    except Exception as e:
        # Catch any other unexpected errors.
        logging.critical(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    try:
        args = _parse_args()
        submit_job(args.job_type, args.payload)
    except SystemExit:
        # argparse handles the --help and invalid argument cases, so we just exit.
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Script execution failed: {e}")
        sys.exit(1)
