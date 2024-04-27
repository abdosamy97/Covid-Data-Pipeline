# Covid-Data-Pipeline

## Overview
This project focuses on analyzing COVID-19 data using Cloudera, Hive, Oozie, and Power BI. We'll cover data ingestion, transformation, visualization, and reporting.

## Business Requirements

1. Show the top 10 ranking countries in death rate on a map.
2. Show the top 10 ranking countries in testing rate on a map.
3. Show the top 10 ranking countries in testing rate on a pie chart.
4. Add a custom chart of choice in the empty section of the dashboard.


### used tools:
- **VMware workstation player**: Software for running virtual machines on your computer.
- **WinSCP**: A popular SFTP and SCP client for Windows that enables secure file transfers between local and remote computers.
- **Cloudera VM**: A pre-configured virtual machine with Hadoop ecosystem components like HDFS, Hive, and Oozie.

## Steps

1. **Environment Setup**:
   - Install VMware Player and set up Cloudera VM.
   - Use WinSCP to transfer files between your PC and Cloudera VM.

2. **File Creation in Cloudera Home**:
   - Create necessary files within your Cloudera home directory using the command line.

3. **Shell Script for Data Upload to HDFS**:
   - Prepare a shell script ("Load_COVID_TO_HDFS.sh") to upload COVID data to HDFS.

4. **Hive Script ("covid_data_preparation.HQL")**:
   - Create three Hive tables:
     - First Hive staging table: Points to the dataset location for data selection.
     - Second Hive ORC table: Partitioned by country, dynamically loads data for faster queries.
     - Third final Hive table: Generates the report for visualization.

5. **Oozie Job Scheduling**:
   - Schedule the execution of the shell script and Hive script using Oozie.

6. **Run the Oozie Job**:
   - Execute the Oozie job to process the data.

7. **Data Transfer from HDFS to Local Directory**:
   - Move data from the "Covid_final_output" table in HDFS to your local directory at `/home/cloudera/covid_project/`.

8. **Transfer Data to Your PC**:
   - Use WinSCP to transfer the data file from Cloudera VM to your PC.

9. **Power BI Visualization**:
   - Create visualizations in Power BI based on the technical and business requirements.
   - Consider using "Line & Stacked Column Charts" to show the relationship between tests and deaths.

10. **Dashboard Screenshot**:
    - Capture a screenshot of your completed dashboard and save it as "dashboard.png."
