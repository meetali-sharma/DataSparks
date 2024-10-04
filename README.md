# DataSpark: Illuminating Insights for Global Electronics

DataSpark is a project focused on deriving valuable insights for the global electronics industry. This project leverages Power BI for interactive data visualization and MSSQL for data management.

## Features
- **Data Cleaning**: Handles missing values using Pandas, ensuring data quality.
- **Database Integration**: Saves cleaned data into an MSSQL database.
- **Interactive Dashboard**: Provides an intuitive dashboard built in Power BI for data analysis and insights.

## Prerequisites
Before starting, ensure you have the following installed:
- Python 3.7 or higher
- MSSQL Server
- Required Python packages (install via the command below):

  ```bash
  pip install -r requirements.txt
  ```

## Packages Used

### 1. **Pandas**
Pandas is used for handling, cleaning, and manipulating the data. It simplifies processes such as identifying and filling missing values, and exporting data to the MSSQL database.

### 2. **ydata-profiling**
This library is used to generate detailed profiling reports for the dataset. The `ProfileReport` class from `ydata_profiling` helps in identifying missing values and other key statistics of the dataset.

### 3. **pyodbc**
`pyodbc` provides a bridge between Python and ODBC-compliant databases like MSSQL. It facilitates the connection and data insertion into the MSSQL database.

### 4. **timedelta**
`timedelta` is a utility from Python's `datetime` module. It helps in representing and working with differences between two points in time.

## Project Workflow

1. **Data Ingestion**  
   Load the dataset into a Pandas DataFrame for further processing.

2. **Data Profiling**  
   Generate an in-depth dataset report using `ydata-profiling` to identify missing values and key metrics.

3. **Data Cleaning**  
   Analyze missing values and handle them appropriately (e.g., using Pandas' `fillna()` function).

4. **Database Connection**  
   Establish a connection to the MSSQL server using `pyodbc` and prepare the database for data insertion.

5. **Data Insertion**  
   Insert the cleaned and preprocessed data into the MSSQL database.

6. **Power BI Dashboard**  
   Create an interactive Power BI dashboard to visualize and explore the dataset insights. The dashboard is built using SQL queries to extract and visualize key insights.
