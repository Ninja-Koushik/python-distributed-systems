import socket
import json
import uuid

# The host and port of the master server
HOST = '127.0.0.1'
PORT = 65432

def create_job(job_type, payload):
    """Creates a job dictionary with a unique ID."""
    return {
        'job_id': str(uuid.uuid4()),
        'type': job_type,
        'payload': payload
    }

def client_main():
    """Main function for the client."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Connected to master.")
        
        # Create a list of sample jobs to send
        jobs_to_send = [
            create_job('fibonacci', 30),
            create_job('fibonacci', 32),
            create_job('fibonacci', 25),
            create_job('fibonacci', 35),
            create_job('fibonacci', 28)
        ]
        
        for job in jobs_to_send:
            try:
                s.sendall(json.dumps(job).encode('utf-8'))
                print(f"Sent job {job['job_id']}")
                # A simple client won't wait for results, but will just send the jobs.
            except socket.error as e:
                print(f"Failed to send job: {e}")
                break

if __name__ == '__main__':
    client_main()
