# Ninja-Nucleus: A Fault-Tolerant Distributed Job Queue

Welcome to **Ninja-Nucleus**, a showcase of a robust and resilient distributed system built to handle mission-critical tasks. This project is a testament to the power of modern software engineering principles, demonstrating how to build a scalable and reliable system from the ground up.

-----

### Key Features and Capabilities

  * **Zero-Loss Fault Tolerance:** This system is designed to never lose a job. It implements **job persistence** by saving the entire queue to a durable file, ensuring seamless recovery from master node crashes.
  * **Intelligent Health Monitoring:** The master node actively monitors its worker fleet using a **heartbeat mechanism**. If a worker fails or disconnects mid-task, the master automatically detects the failure and **re-queues the job** for another worker to process.
  * **Dynamic Scalability:** The architecture is designed for infinite scalability. A dedicated client can submit new jobs to the queue at any time, and any number of workers can join the system to tackle the workload without any configuration changes.
  * **Asynchronous & High-Performance Communication:** Built on **gRPC**, a cutting-edge, high-performance RPC framework, this system communicates with unparalleled speed and efficiency.
  * **Container-Ready Design:** The codebase is lightweight and modular, making it perfectly suited for **containerization** with tools like Docker for easy deployment on any cloud platform.

-----

### The Architecture: How It Works

Ninja-Nucleus operates on a simple yet powerful master-worker architecture. The master node is the brain, managing a persistent job queue and a fleet of workers. Workers act as the muscle, requesting jobs from the master, performing the work, and returning the results.

<img width="847" height="567" alt="architecture" src="https://github.com/user-attachments/assets/8ee78291-12f9-4720-94cf-823b51816a68" />

1.  **Client:** The client submits a new job to the master using the `SendJob` RPC.
2.  **Master:** The master adds the job to its persistent queue, ready for dispatch.
3.  **Workers:** Workers continuously check in with the master via heartbeats and request new jobs.
4.  **Job Execution:** The master dispatches a job to an available worker.
5.  **Results:** Once the job is complete, the worker sends the result back to the master.

-----

### Getting Started: Unleash the Power

To run this project, follow these simple steps.

1.  **Clone the repository and install dependencies:**
    ```bash
    git clone [Your Repository URL Here]
    cd [Your Project Directory]
    pip install -r requirements.txt
    ```
2.  **Regenerate gRPC Code:** After modifying the `.proto` file, ensure your Python code is up-to-date.
    ```bash
    python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. job_queue.proto
    ```
3.  **Start the Master:** In one terminal, launch the master node.
    ```bash
    python3 master.py
    ```
4.  **Start a Worker:** In a separate terminal, launch a worker to begin processing jobs.
    ```bash
    python3 worker.py worker-1
    ```
5.  **Submit a Job:** In a third terminal, use the client script to submit a new, dynamic job.
    ```bash
    python3 client.py fibonacci 42
    ```

-----

### Future Enhancements

  * **Advanced Load Balancing:** Implement a more sophisticated load balancing algorithm (e.g., least-loaded, round-robin) for optimal job distribution.
  * **Database Integration:** Migrate the persistent state from a JSON file to a scalable database like Redis or PostgreSQL.
  * **Service Discovery:** Integrate a service discovery system to allow workers to automatically find the master.

This project is a testament to the power of resilient system design. Feel free to explore and contribute\!
