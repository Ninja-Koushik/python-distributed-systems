"""
Client.py
Description: This script acts as a client that submits new jobs to the master node
in a distributed job queue system.
"""

import grpc
import sys
import uuid
import logging

# Import the generated gRPC files
import job_queue_pb2
import job_queue_pb2_grpc

def run_client(job_id, job_type, payload):
    """
    Creates and sends a single job request to the master.

    Args:
        job_id (str): A unique identifier for the job.
        job_type (str): The type of job (e.g., "fibonacci").
        payload (int): The input data for the job.
    """
    # Create an insecure channel to connect to the master
    with grpc.insecure_channel('localhost:50051') as channel:
        # Create a stub (client) to call the master's services
        stub = job_queue_pb2_grpc.JobQueueStub(channel)

        # Create the JobRequest message with the provided data
        job_request = job_queue_pb2.JobRequest(
            job_id=job_id,
            job_type=job_type,
            payload=payload
        )
        
        logging.info(f"Submitting job: ID={job_id}, Type={job_type}, Payload={payload}")
        try:
            # Call the SendJob RPC method on the master
            response = stub.SendJob(job_request)
            logging.info(f"Master's response: {response.message}")
        except grpc.RpcError as e:
            logging.info(f"Could not connect to master: {e}")

if __name__ == '__main__':
    # Check if command-line arguments are provided
    if len(sys.argv) < 3:
        logging.info("Usage: python3 client.py <job_type> <payload>")
        sys.exit(1)

    # Generate a unique job ID
    job_id = f"client_job_{uuid.uuid4()}"
    job_type = sys.argv[1]
    payload = int(sys.argv[2])

    run_client(job_id, job_type, payload)
