# Fabric_Airbnb_Data_Engineering_Project

This project demonstrates the capabilities of Microsoft Fabric using Airbnb datasets ingested through Microsoft Fabric Data Factory and stored in a Lakehouse following the Medallion Architecture (Bronze, Silver, and Gold layers). Data transformations were performed using Fabric Notebooks, with a dynamic SCD Type 2 implemented in the Gold layer. The final Gold layer was modelled using a Star Schema to enable efficient analytics and reporting.

<img width="2517" height="1080" alt="Fabric_Overview" src="https://github.com/user-attachments/assets/adc2ec32-d136-46ab-b91d-61629f323244" />


# Languages
Python and SQL

# Architecture Summary

Data Source:
Data was ingested via RESTAPI .

# Data Ingestion: Azure Data Factory
Fabric Data Factory (FDF) orchestrates and manages data ingestion activities.

Raw data is ingested into Onelake.

Ingestion was delivered to the staging layer in preparation for data transformation.

Pipelines use triggers, datasets, and linked services to securely move data.

Unlike Logic Apps via Azure Datafactory Pipeline was monitored via email activity.

# Enriched Layer Transformations

Transformations include modular cleaning, enrichment, and business logic application.

# Curated Gold Layer: SCD TYPE 2 AND STAR SCHEMA
Since the project was done within Fabric and not Databricks I could not utilise DLT but neverthless manual application of 

Slowly Changing Dimensions (SCD Type 2) are applied to dimension tables for historical tracking.

Upserts (SCD Type 1) are applied to fact tables to keep data up to date. 

# Phase 1 Ingestion
<img width="1440" height="716" alt="Screenshot 2026-01-05 at 15 23 09" src="https://github.com/user-attachments/assets/beb47b99-b21d-45d2-bc4c-a73437815290" />
Our Workspace


<img width="1440" height="663" alt="Screenshot 2026-01-05 at 16 16 30" src="https://github.com/user-attachments/assets/f5f5e55a-fbde-492b-9d2e-c28f42f950dd" />
<img width="1440" height="663" alt="Screenshot 2026-01-05 at 16 20 06" src="https://github.com/user-attachments/assets/0b5e49a4-8397-4930-8d0d-e94e2d3bee54" />
While utilising Fabric Data Factory, I observed clear differences in pipeline orchestration, which required some adaptation. For this project, the main focus was on effectively using ForEach activities and parameterisation. I implemented a JSON control table containing all parameterised entities and stored it in OneLake. A Lookup activity was then used to read this configuration and dynamically drive the ForEach activity, enabling the successful ingestion of all datasets into their respective folders in the OneLake.

<img width="1440" height="663" alt="Screenshot 2026-01-05 at 16 19 34" src="https://github.com/user-attachments/assets/c3191e20-8c15-4928-afa0-5a4beafd3567" />
Once the pipeline was successfully implemented, I added further validation by configuring an Email activity to trigger on failure, enabling easy monitoring and prompt notification in the event of any pipeline issues.

# Phase Enriched Silver Transformations

In the second phase of the project, I focused on data cleaning and transformations. This primarily involved adding new columns to provide analytical context and casting data types, particularly for our datasets ingested in Parquet format. Through these cleaning and transformation steps, I was able to derive meaningful insights using Fabric Notebooks.

<img width="1440" height="635" alt="Screenshot 2026-01-08 at 05 42 12" src="https://github.com/user-attachments/assets/351924d2-59c1-4b94-984d-35e40afeb540" />

<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 35 52" src="https://github.com/user-attachments/assets/25a2d73a-12cf-4849-b0be-b59a37e574c6" />
<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 37 55" src="https://github.com/user-attachments/assets/1ad0aeec-095e-4c48-af58-e036d1b9a90c" />

<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 38 10" src="https://github.com/user-attachments/assets/7aaa35a5-ea44-48e0-9fd7-10af5bdcca4a" />
After completing data cleaning and transformations, the enriched datasets were written to the Silver Lakehouse and materialised as Silver tables to support the upcoming SCD Type 2 implementation. Since Microsoft Fabric automatically enables optimisations such as OPTIMIZE and Z-ORDER, there was no need to configure optimizeWrite, making the process more seamless and efficient.

<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 39 10" src="https://github.com/user-attachments/assets/e2e2b1be-7677-4da9-9bb9-c2d822a5c8ed" />


# Phase 3 Curated Gold Layer(SCD Type 2 And Star Schema)
To finalise the project, I implemented a dynamic dimensional modelling with a primary focus on SCD Type 2 to enable historical tracking(So in further essence an SCD Type 2 builder all in one notebook). Following this, I curated the fact table to support efficient joins and downstream analytics.
<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 55 21" src="https://github.com/user-attachments/assets/790a4d14-a16c-49a8-b160-e361d60425c2" />

<img width="1440" height="663" alt="Screenshot 2026-01-08 at 04 55 51" src="https://github.com/user-attachments/assets/4db1275c-583e-4622-b56e-9f173d2f78c5" />

And then for our fact table I employed SCD TYPE 1 for updates and inserts.

<img width="1440" height="437" alt="Screenshot 2026-01-08 at 04 58 08" src="https://github.com/user-attachments/assets/176b3feb-41f2-456a-9bf4-f6b83d6db052" />

At this stage, the curated datasets were finalised and written to the Gold Lakehouse. Tables were created over the curated files to enable seamless CTAS operations when loading data into the designated Fabric Data Warehouse.

# Loading To Fabric Data Warehouse

Lakehouse View Of Our Curated Datsets
<img width="1440" height="663" alt="Screenshot 2026-01-07 at 02 46 09" src="https://github.com/user-attachments/assets/97f9046c-869d-439d-bb36-0c99f073b2a1" />

<img width="1440" height="663" alt="Screenshot 2026-01-07 at 02 39 54" src="https://github.com/user-attachments/assets/9b6dc451-aa68-4366-bcd2-1ff435cead5e" />
<img width="1440" height="663" alt="Screenshot 2026-01-07 at 02 46 39" src="https://github.com/user-attachments/assets/e06426a5-7261-45b9-a5f9-301d8925a57f" />




Curated datasets were easily loaded into the warehouse, with CTAS used to create structured tables and schemas for better control, the Lakehouse could be directly added as well to the warehouse. However, I chose to create specific schemas and tables using CTAS for better structure and control. Afterward, I validated the curated datasets using SQL queries and proceeded to visualise the data for analysis.


<img width="1440" height="635" alt="Screenshot 2026-01-08 at 05 14 53" src="https://github.com/user-attachments/assets/7cfee387-2fc4-4856-8200-5282c464bce4" />


<img width="1440" height="635" alt="Screenshot 2026-01-08 at 05 12 55" src="https://github.com/user-attachments/assets/9fe2fbdb-4c36-4534-a896-acbd4a5a21f6" />

<img width="1313" height="663" alt="Screenshot 2026-01-07 at 01 17 59" src="https://github.com/user-attachments/assets/f8f2e2a9-8d86-49b4-9c7b-98963e2cc9b3" />
<img width="1440" height="635" alt="Screenshot 2026-01-08 at 05 08 09" src="https://github.com/user-attachments/assets/b7f51bfd-5c5d-425e-8804-d134bc03ac6d" />


# Data Visualisation

After loading the data, I visualized our curated and newly stored Airbnb datasets to explore insights and validate the transformations.

<img width="1440" height="663" alt="Screenshot 2026-01-07 at 01 17 23" src="https://github.com/user-attachments/assets/d2c4007b-042e-4824-8819-e4117875a1aa" />

<img width="1313" height="663" alt="Screenshot 2026-01-07 at 01 20 18" src="https://github.com/user-attachments/assets/8c8122cf-2a1d-4202-aa65-8031c3d97ebb" />
<img width="1440" height="663" alt="Screenshot 2026-01-07 at 01 23 55" src="https://github.com/user-attachments/assets/d7f28825-ba2d-472d-b5c9-312842f746e9" />

# Semantic Model 

This semantic model visualises the Star Schema, highlighting the relationships between the fact table and its related dimension tables for efficient analytics and reporting.
<img width="1440" height="663" alt="Screenshot 2026-01-07 at 01 34 46" src="https://github.com/user-attachments/assets/ac6cff77-ebae-4541-b801-3548dea7cd23" />

# Data Lineage
This diagram illustrates our workspace and complete data lineage, tracing the flow from data ingestion through transformations to the final loading of curated datasets into the Fabric Data Warehouse, highlighting how each stage contributes to analytics-ready data.

<img width="1440" height="663" alt="Screenshot 2026-01-08 at 02 12 07" src="https://github.com/user-attachments/assets/1f8eafc4-3f7c-492e-aebc-7cb1a08defa2" />


# Take Aways

In my honest opinion, integrating Databricks with this project could have streamlined pipeline development, as idempotency is key in modern data engineering. For example, using Autoloaders for the Silver layer would handle schema evolution and track changes automatically. In the curated layer, leveraging DLT (now Spark Declarative Pipelines - SDP) with Auto CDC Flow would automate SCD Type 2, saving time on long code and simplifying the management of historical data.

Nevertheless, this does not diminish the efficiency and value of Fabric. As the saying goes:

Manual SCDs give you headaches managing.

DLT lets you sleep.

That’s the main takeaway from this experience.
