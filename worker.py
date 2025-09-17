import socket
import json

# The host and port of the master server
HOST = '127.0.0.1'
PORT = 65432

def fibonacci(n):
    """Calculates the nth Fibonacci number."""
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

def worker_main():
    """Main function for the worker node."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Connected to master. Waiting for job...")
        
        while True:
            # Receive job from master
            data = s.recv(1024)
            if not data:
                print("Master disconnected.")
                break
            
            job = json.loads(data.decode('utf-8'))
            job_id = job.get('job_id')
            job_type = job.get('type')
            
            if job_type == 'fibonacci':
                n = job.get('payload')
                print(f"Executing job: fibonacci({n})")
                
                result = fibonacci(n)
                
                # Send result back to master
                response = {'job_id': job_id, 'result': result}
                s.sendall(json.dumps(response).encode('utf-8'))
                print(f"Job {job_id} complete. Result sent.")
            else:
                print(f"Unknown job type: {job_type}")

if __name__ == '__main__':
    worker_main()
