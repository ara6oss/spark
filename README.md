## Setup

### Requirements

To run this project, you need the following tools:

- **Docker**: To build and run the containerized application.
- **Python 3.x**: Required for PySpark and other dependencies.
- **Apache Spark**: Should be installed in the environment (Docker setup handles this).
- **geohash2**: Python library for geohashing.

### Installing Dependencies

To set up the project, clone the repository and install the required dependencies.

1. Clone the repository:

   ```bash
   git clone https://github.com/ara6oss/spark
   cd spark

2. Build the Docker container:
   
   ```bash
   docker build -t spark_etl .

3. Run the Docker container:

   ```bash
   docker run -it -v $(pwd):/app spark_etl /bin/bash

4. Run Tasks:

   ```bash
   spark-submit etl_job.py
   spark-submit geohash.py
   spark-submit etl_job.py
   spark-submit left-join.py
   spark-submit last.py
   

   
