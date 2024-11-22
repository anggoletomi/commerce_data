---

# Automated Data Reporting and Visualization

This project focuses on automating the process of data reporting by integrating and combining multiple data sources. It aims to streamline workflows, centralize data storage, and generate interactive dashboards for insightful and actionable reporting.

## **Overview**

The solution automates the entire workflow, starting from data extraction to dashboard visualization:

1. **Data Extraction**:  
   Raw data is extracted automatically from various sources using Robotic Process Automation (RPA) tools like **Power Automate**. These raw files are in formats such as Excel or CSV.

2. **Data Storage**:  
   Extracted files are automatically saved in: Cloud storage (**Google Drive**) or a virtual machine / remote server for centralized access.  

3. **Data Processing**:  
   The raw files are:
   - Processed using **Python** to clean and load the data into a centralized database (**BigQuery**).  
   - This ensures that the original data is preserved and easily accessible.

4. **Report Generation**:  
   - **Python** scripts are used to transform manual logic and calculations into automated workflows.
   - A final processed dataset (table) is created and loaded back into the database for reporting.

5. **Data Visualization**:  
   - The final tables are connected to **BI tools** like **Looker Studio**, **Tableau**, or **Power BI**. 

## **Key Features**
- Fully automated workflow from data extraction to dashboard creation.
- Seamless integration with multiple data sources.
- Centralized data storage and database.
- Scalability with cloud platforms and virtual machines.
- Interactive dashboards for dynamic reporting.

## **Technology Stack**
- **Programming Language**: Python  
- **Data Sources**: Excel, CSV
- **Cloud Platforms**: Google Cloud Platform (GCP), Virtual Machines, Google Drive 
- **Databases**: BigQuery  
- **BI Tools**: Looker Studio, Tableau, Power BI  
- **Automation Tools**: Power Automate, Cronjobs  

## **Benefits**
- Saves time by automating manual reporting tasks.  
- Reduces errors with consistent and accurate data processing.  
- Improves accessibility and collaboration with centralized data storage.  
- Enhances decision-making through interactive dashboards.

---
