# Databricks notebook source
# MAGIC %md
# MAGIC # Ex2 - Getting and Knowing your Data

# COMMAND ----------

# MAGIC %md
# MAGIC This time we are going to pull data directly from the internet.
# MAGIC Special thanks to: https://github.com/justmarkham for sharing the dataset and materials.
# MAGIC 
# MAGIC ### Step 1. Import the necessary libraries

# COMMAND ----------

import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Import the dataset from this [address](https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv). 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Assign it to a variable called chipo.

# COMMAND ----------

df = pd.read_csv('https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv', sep='\t')

# COMMAND ----------

kdf = ks.from_pandas(df)

# COMMAND ----------

chipo = df

# COMMAND ----------

display(kdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4. See the first 10 entries

# COMMAND ----------

chipo.head(10)

# COMMAND ----------

kdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5. What is the number of observations in the dataset?

# COMMAND ----------

# Solution 1

chipo.shape

# COMMAND ----------

chipo.shape[0]

# COMMAND ----------

# Solution 2

chipo.count()[0]

# COMMAND ----------

chipo.info()

# COMMAND ----------

kdf.shape

# COMMAND ----------

kdf.shape[0]

# COMMAND ----------

kdf.count()

# COMMAND ----------

kdf.info()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6. What is the number of columns in the dataset?

# COMMAND ----------

chipo.shape[1]

# COMMAND ----------

kdf.shape[1]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7. Print the name of all the columns.

# COMMAND ----------

chipo.columns

# COMMAND ----------

kdf.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8. How is the dataset indexed?

# COMMAND ----------

chipo.index

# COMMAND ----------

kdf.index

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9. Which was the most-ordered item? 

# COMMAND ----------

c = chipo.groupby('item_name')
c = c.sum()
c = c.sort_values(['quantity'], ascending = False)
c.head()

# COMMAND ----------

k = kdf.groupby('item_name')
k = k.sum()
k = k.sort_values(['quantity'], ascending = False)
k.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10. For the most-ordered item, how many items were ordered?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 11. What was the most ordered item in the choice_description column?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 12. How many items were orderd in total?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 13. Turn the item price into a float

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 13.a. Check the item price type

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 13.b. Create a lambda function and change the type of item price

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 13.c. Check the item price type

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 14. How much was the revenue for the period in the dataset?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 15. How many orders were made in the period?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 16. What is the average revenue amount per order?

# COMMAND ----------

# Solution 1



# COMMAND ----------

# Solution 2



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 17. How many different items are sold?

# COMMAND ----------

