# Introduction to DataPipelines

The DataPipelines project is a set of data pipelines developed  with the aim of automating the process of extracting, loading, transforming, and processing data from various data sources. The pipelines also include functions for scoring data and returning limits to clients.

The project is designed to be deployed on Apache Airflow, a popular platform for authoring, scheduling, and monitoring workflows. Once the installation is successful, the pipelines can be accessed and monitored through the Airflow user interface.

This project plays a crucial role in ensuring that data  is processed efficiently and accurately. By automating these processes, manual errors are eliminated and the time and effort required to perform these tasks is reduced. The project also provides insights into the data processing activities through the Airflow user interface, allowing for better tracking and monitoring of the pipelines.

# Project Installation

## Cloning the Project

1. Open a terminal and navigate to the directory where you want to clone the project.
2. Clone the project using the following command:

  
  


## Creating a Python 3 Virtual Environment

1. Navigate to the project directory:

   cd DataPipelines

2. Create a virtual environment using the following command:
  
   python3 -m venv env


## Activating the Virtual Environment

1. Activate the virtual environment using the following command:

   source env/bin/activate


## Installing Required Packages

1. Install the required packages using the following command:

   pip install -r requirements.txt

2. In your airflow.cfg file, set the following:

   dags_folder = /path/to/DataPipelines/dags


The installation is now complete, and the application is ready to use.
