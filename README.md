# IFCO DE Challenge

This project is part of the IFCO Data Engineering Challenge. It includes data analysis and visualization tasks using PySpark, containerized with Docker for easy setup and execution.

## Table of Contents
* Prerequisites

* Setup Instructions

* Running the Project

* Project Structure


## Prerequisites
Before setting up the project, ensure you have the following installed:

* Docker (with Docker Compose)

* Git (for cloning the repository)

## Setup Instructions
1. Clone the Repository
Clone the repository to your local machine:

```
bash

git clone https://github.com/victron777/ifco_de_challenge.git
cd ifco_de_challenge
```

2. Build and Run the Docker Containers
Use Docker Compose to build and start the project:

```
bash

docker-compose build
docker-compose up
```
This will:

* Build the Docker image for the project.

* Start the container and run the analysis script.

## Running the Project
Once the Docker container is running:

* The analysis script will execute automatically.

* Visualizations will be generated in Streamlit dashboard.

To view the output:

1. Open your browser.

2. Go to this URL: http://0.0.0.0:8501

## Project Structure
The project is organized as follows:

```
Copy
ifco_de_challenge/
├── data/                   # Dataset files (e.g., orders.csv, invoices.json)
├── src/                    # Source code
│   ├── main.py             # Main script for analysis
│   ├── analysis.py         # Functions for data analysis
│   └── visualization.py    # Functions for data visualization
├── output/                 # Generated output files (e.g., plots, reports)
├── Dockerfile              # Docker configuration
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```