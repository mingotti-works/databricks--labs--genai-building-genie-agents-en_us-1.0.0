# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Building Single Agents with Genie
# MAGIC
# MAGIC This demonstration showcases how to build and deploy a Genie Agent that leverages Unity Catalog functions for natural language data querying. You will learn to create table-valued functions that work seamlessly with Genie and perform a basic configuration of a Genie space.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Create Unity Catalog functions for Genie integration
# MAGIC - Convert scalar functions to table-valued functions for Genie compatibility
# MAGIC - Configure and test a Genie space with custom functions
# MAGIC - Query Genie spaces using natural language and interpret the results and view the tool-calling capabilities of the agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cells to configure your working environment for this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Compute Requirements
# MAGIC
# MAGIC **🚨 REQUIRED - SELECT SERVERLESS COMPUTE**
# MAGIC
# MAGIC This course has been configured to run on Serverless compute. While classic compute may also work, testing has been performed on serverless.
# MAGIC
# MAGIC This demo was tested using version 4 of Serverless compute. To ensure that you are using the correct version of Serverless, please navigate to the **Environment** button on the right and open it (see screenshot below).
# MAGIC
# MAGIC ![optional alt text](./Includes/images/serverless-version.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Additionally, we will be using a SQL Warehouse, so please make sure you have one you can use with your Genie Agent.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Install Dependencies
# MAGIC
# MAGIC As part of the workspace setup, several Python libraries will need to be installed. Run the next cell to do so. This example uses LangChain, but a similar approach can be applied to other libraries.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Configure Catalog and Schema
# MAGIC
# MAGIC **🚨 NOTE:** You will need to update the following cell to your catalog name. The schema name will be created automatically for you based on the course name.
# MAGIC
# MAGIC **🚨 NOTE:** If you are using **Vocareum**, your catalog has already been configured for you and is of the form **labuserXXX_XXX**, which matches your Vocareum username. You should set this as your catalog name.
# MAGIC > Example: catalog_name = "labuser31415926_5358979323"
# MAGIC
# MAGIC The catalog and schema variables are used throughout this notebook when referencing Unity Catalog assets.

# COMMAND ----------

# Used when needing to pass catalog/schema name with Python
catalog_name = "labuser14830110_1777001490"
schema_name = "genai_genie_agent"
schema_name = dev_lab_setup(catalog_name, schema_name) # Store the catalog and schema as catalog_name and schema_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### A4. Our Data
# MAGIC This demonstration relies on the Airbnb dataset from Databricks Marketplace. Note that you may already have access to the Airbnb dataset.
# MAGIC #### Vocareum: You have access
# MAGIC If you launched this demo in a Vocareum environemnt, you will automatically have access to the Delta share. It is called `dbacademy_airbnb_sample_data`. For Vocareum users, you will set 
# MAGIC >`databricks_share_name=dbacademy_airbnb_sample_data`. 
# MAGIC #### Non-Vocareum: Check if you have access
# MAGIC Check in the **Catalog Explorer** by searching for `databricks_airbnb_sample_data`. Provided you have the proper level of permisisons on this delta share, you can update the next cell to read 
# MAGIC >`databricks_share_name=databricks_airbnb_sample_data`. 
# MAGIC
# MAGIC ##### I don't have access/can't see the dataset
# MAGIC If you don't have access or can't see the dataset in your **Catalog Explorer**, the next set of instructions will help walk you through how to get this dataset in your workspace.
# MAGIC
# MAGIC 1. Navigate to Marketplace and search **Airbnb Sample Data** and click on the tile that reads **Airbnb Sample Data**.
# MAGIC 1. Next, click **Get instant access** and follow the on-screen instructions to bring that dataset in.
# MAGIC 1. Create a unique Databricks share name. If a name is already in use, you will need to use a different name. Copy the same name into the cell below. For example:
# MAGIC >`databricks_share_name=<unique_name>`. 

# COMMAND ----------

## TODO
databricks_share_name = "dbacademy_airbnb_sample_data" # Delta share name

# COMMAND ----------

# MAGIC %md
# MAGIC 1. As a part of the classroom setup, a helper function has been configured for you process the dataset from the Delta share. Run the cell to process the CSV `sf-airbnb.csv` from the Airbnb Delta share volume `v01`. 

# COMMAND ----------

df = process_csv(databricks_share_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Trusted Assets for the Genie Agent
# MAGIC
# MAGIC Next, we will register a trusted asset for our Genie to consume. A trusted asset is a predefined function or example query that's meant to provide verified answers to questions that you anticipate from users. That is, when a user submits a question that invokes a trusted asset, it's indicated in the reponse, which adds an extra layer of assurance to the accuracy of the results.
# MAGIC
# MAGIC When adding a trusted asset, a user with at least **CAN EDIT** permission on the Genie space can add it ot the Genie Space. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Creating a Basic UC Function
# MAGIC
# MAGIC Here's a typical SQL query for building a Unity Catalog function that returns a `INT` value. This function calculates the average price by neighborhood for San Francisco Airbnb listings. While functional, UC functions must return type TABLE to be used with the Genie agent. 
# MAGIC
# MAGIC > If you attempt to attach the function `get_average_price_by_neighborhood`, you will receive an error detailing the returned type must be `TABLE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_average_price_by_neighborhood(
# MAGIC   neighborhood_name STRING COMMENT "The neighborhood name to filter by (e.g., 'Mission', 'Upper Market')"
# MAGIC )
# MAGIC RETURNS INT
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Calculates the average listing price for a specific neighborhood in San Francisco. Returns the average price as a numeric value. Price strings are cleaned and converted to numeric values before averaging.'
# MAGIC RETURN 
# MAGIC SELECT AVG(CAST(REGEXP_REPLACE(price, '[^0-9.]', '') AS DOUBLE)) AS average_price
# MAGIC FROM sf_airbnb_listings
# MAGIC WHERE neighbourhood_cleansed = neighborhood_name
# MAGIC   AND price IS NOT NULL
# MAGIC   AND REGEXP_REPLACE(price, '[^0-9.]', '') != ''

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Test the UC Function
# MAGIC As a part of incorporating best practices, we test our newly registered function `get_average_price_by_neighborhood` by passing the value `Mission`. The expected outcome is `229`. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT get_average_price_by_neighborhood('Mission')

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Creating a SQL table function
# MAGIC
# MAGIC The query above returns a `INT` datatype, but we're going to make a small edit so that it can be used by the Genie Agent. Simply update `RETURNS INT` → `RETURNS TABLE` and name the column as `average_price` with datatype `INT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION genie_average_price_by_neighborhood(
# MAGIC   neighborhood_name STRING COMMENT "The neighborhood name to filter by (e.g., 'Mission', 'Upper Market')"
# MAGIC )
# MAGIC RETURNS TABLE (average_price INT)
# MAGIC LANGUAGE SQL
# MAGIC DETERMINISTIC
# MAGIC COMMENT 'Calculates the average listing price for a specific neighborhood in San Francisco. Returns the average price as a numeric value. Price strings are cleaned and converted to numeric values before averaging.'
# MAGIC RETURN 
# MAGIC SELECT AVG(CAST(REGEXP_REPLACE(price, '[^0-9.]', '') AS DOUBLE)) AS average_price
# MAGIC FROM sf_airbnb_listings
# MAGIC WHERE neighbourhood_cleansed = neighborhood_name
# MAGIC   AND price IS NOT NULL
# MAGIC   AND REGEXP_REPLACE(price, '[^0-9.]', '') != ''

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. Test the SQL Table Function
# MAGIC Just as did previously, we will test our function before equipping it to our Genie Agent. 
# MAGIC > Note the difference in the syntax since this returns a table and not `INT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM genie_average_price_by_neighborhood('Mission')

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Creating and Configuring a Genie Space
# MAGIC
# MAGIC After creating and testing your Unity Catalog functions, the next step is to create a Genie Agent and configure it properly with your UC functions.
# MAGIC
# MAGIC ### Creating a Genie Space Using the Databricks UI
# MAGIC
# MAGIC To create and configure your Genie space through the Databricks workspace interface:
# MAGIC
# MAGIC 1. **Navigate to the Genie interface** by clicking on **Genie** in the left sidebar of your Databricks workspace
# MAGIC 2. **Create a new space** by clicking the **+ New** button
# MAGIC 3. **Add data sources by** searching **sf_airbnb_listings**, selecting it, and clicking **Create**
# MAGIC     - On the next screen you may need to click on **Start  Warehouse** if your compute is not active (or navigate to **Compute** and spin up your warehouse there). 
# MAGIC 4. Rename the Genie space _Airbnb Genie_
# MAGIC 4. Attach the function we created earlier by clicking on **Configure** at the top right and then **Instructions** > **SQL Queries** > dropdown menu next to **+ Add** > **SQL function** and searching for the UC function `genie_average_price_by_neighborhood` in your catalog and schema (see cell 8 for the catalog and schema you setup during this lab) and click **Save**.
# MAGIC     > If you're new to adding functions, [you can read these docs.](https://docs.databricks.com/aws/en/genie/set-up#how-genie-uses-example-queries)
# MAGIC 5. **(Optional) Configure space settings** by providing a clear description of what the space can help users accomplish
# MAGIC
# MAGIC You are now ready to test the Genie agent with the UC function.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Testing and Inspecting Your Genie Agent
# MAGIC
# MAGIC After attaching the function `genie_average_price_by_neighborhood` to your Genie space, test it with natural language queries. Try asking: _"Get the average price for the Mission neighborhood."_
# MAGIC
# MAGIC The output will display a table showing the average price like we saw earlier. 
# MAGIC
# MAGIC ### Interpreting the Results
# MAGIC In the screenshot below, we see that clicking on **Show code** displays our UC function being queried. Keep in mind that since this is such a simple request, your Genie Agent might create the query on the fly. We can see the returned table value, which is expected because our function returns `TABLE`. 
# MAGIC
# MAGIC > Note the appended message _**Is this correct?**_ The Genie feedback loop is designed as a human-in-the-loop process where user signals (like thumbs up/down, 'Fix It', and request reviews) inform the space author, who then reviews and incrementally improves the Genie space—Genie itself does not auto-tune or learn from feedback directly.
# MAGIC
# MAGIC ![Genie Tool Usage Example](./Includes/images/genie-tool.png)
# MAGIC
# MAGIC Try creating more functions and equipping your Genie Agent with more tools. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Conclusion
# MAGIC
# MAGIC In this demonstration, you successfully built a managed single agent using Genie with the following key accomplishments:
# MAGIC
# MAGIC - **Created Unity Catalog functions** optimized for Genie integration by converting scalar functions to table-valued functions
# MAGIC - **Configured a Genie space** with custom functions and data sources for natural language querying
# MAGIC - **Tested natural language queries** that leverage your custom functions to return structured data
# MAGIC - **Learned integration patterns** for connecting Genie agents with multi-agent supervisor frameworks
# MAGIC
# MAGIC This foundation enables you to build sophisticated AI agents that can understand natural language questions about your data and provide accurate, structured responses using your organization's specific business logic and data sources.
# MAGIC
# MAGIC ### Further Trainings
# MAGIC Please see our [AI/BI for Data Analysts](https://www.databricks.com/training/catalog/aibi-for-data-analysts-3707) and [AI/BI for Self-Service Analytics](https://www.databricks.com/training/catalog/aibi-for-self-service-analytics-3478) courses for further training regarding Genie Agents. 

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>