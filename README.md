# IFCO Data Engineering Challenge

This project is part of the IFCO Data Engineering Challenge. It includes data analysis and visualization tasks using PySpark, containerized with Docker for easy setup and execution.

## Table of Contents
* Prerequisites
* Setup Instructions
* Running the Project
* Project Structure
* Thoughtful Assumptions and Explanations


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

To view the dashboard:

1. Open your browser.

2. Go to this URL: http://0.0.0.0:8501

## Project Structure
The project is organized as follows:

```
ifco_de_challenge/
├── data/                   # Dataset files (e.g., orders.csv, invoices.json)
├── src/                    # Source code
│   ├── main.py             # Main script for analysis
│   ├── analysis.py         # Functions for data analysis
│   ├── dashboard.py        # Functions for data visualization
│   ├── config.py           # Constant functions to define the base path and folders
│   ├── data_loader.py      # Functions to load input data from source
└── └── utils.py            # Utilities used in the main app
├── output/                 # Generated output files (e.g., plots, reports)
├── Dockerfile              # Docker configuration
├── docker-compose.yml      # Docker Compose configuration
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
└── tests/                  # unit test code
    └── test_analysis.pyy    
```

## Thoughtful Assumptions and Explanations

This section outlines the key assumptions and reasoning behind the modeling approach used in this project.

---

### 1. **Data Quality and Completeness**
- **Assumption**: The provided datasets (`orders.csv` and `invoices.json`) are complete and accurate.
- **Explanation**:
    - Missing values in the `contact_data` column were handled by treating them as `NULL`.
    - Duplicate entries in the `invoices.json` dataset (e.g., two invoices for the same `order_id`) were assumed to be valid and retained for analysis.

### 2. **Crate Type Analysis**
- **Assumption**: The `crate_type` column in `orders.csv` is consistent and correctly categorized into `Plastic`, `Wood`, and `Metal`.
- **Explanation**:
    - The distribution of orders by crate type was analyzed to understand the popularity of each type.
    - Focus was placed on `Plastic` crates as per the stakeholder's request.

### 3. **Sales Owner Performance**
- **Assumption**: The `salesowners` column in `orders.csv` contains a comma-separated list of sales owners for each order.
- **Explanation**:
    - The column was split into individual sales owners using `explode` to analyze performance.
    - Sales owners with fewer plastic crate orders in the last 12 months were identified as needing additional training.

### 4. **Visualization Approach**
- **Assumption**: Visualizations should be simple and intuitive for stakeholders to interpret.
- **Explanation**:
    - Bar charts were used to show the distribution of orders by crate type and sales owner performance.
    - Line charts were used to display the top 5 performers over time, as they effectively show trends.

### 5. **Handling Duplicate Invoices**
- **Assumption**: Duplicate invoices for the same `order_id` in `invoices.json` are valid and represent multiple transactions.
- **Explanation**:
    - Duplicates were retained to ensure all transactions were included in the analysis.
    - Aggregation functions (e.g., `sum`) were used to handle duplicate values where necessary.

### 6. **Time-Based Filtering**
- **Assumption**: The last 12 months of data are sufficient to identify trends and training needs.
- **Explanation**:
    - A 12-month window was chosen to balance recency and data volume.
    - Older data was excluded to focus on recent performance.

### 7. **Docker Environment**
- **Assumption**: The project will be run in a Docker container to ensure consistency across environments.
- **Explanation**:
    - Docker was used to encapsulate dependencies and avoid conflicts with local environments.
    - The `Dockerfile` and `docker-compose.yml` files were configured to simplify setup and execution.