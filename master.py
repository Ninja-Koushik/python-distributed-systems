import socket
import json
import threading

# The host and port for the master server
HOST = '127.0.0.1'
PORT = 65432

job_queue = []
jobs_in_progress = {}

def handle_worker(conn, addr):
    """Handles communication with a single worker node."""
    print(f"Connected by worker at {addr}")
    while job_queue:
        # Get the next job from the queue
        job = job_queue.pop(0)
        job_id = job['job_id']
        jobs_in_progress[job_id] = job
        
        try:
            # Send the job to the worker
            conn.sendall(json.dumps(job).encode('utf-8'))
            print(f"Sent job {job_id} to worker")
            
            # Wait for the result
            data = conn.recv(1024)
            if data:
                response = json.loads(data.decode('utf-8'))
                print(f"Received result for job {response['job_id']}: {response['result']}")
                # Clean up the job from the in-progress list
                del jobs_in_progress[response['job_id']]
        except (socket.error, json.JSONDecodeError) as e:
            # Handle worker failure
            print(f"Worker {addr} failed: {e}. Re-queuing job {job_id}.")
            job_queue.insert(0, job)
            break

def master_main():
    """Main function to run the master server."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen()
    print("Master is listening for workers...")

    # Initialize the job queue with some sample jobs
    for i in range(10):
        job_queue.append({'job_id': f'job_{i}', 'type': 'fibonacci', 'payload': 30})
    print(f"Job queue initialized with {len(job_queue)} jobs.")
    
    # Accept and handle one worker connection for this example
    # For a real system, this would be in a loop to handle multiple workers
    conn, addr = s.accept()
    handle_worker(conn, addr)
    
if __name__ == '__main__':
    master_main()
