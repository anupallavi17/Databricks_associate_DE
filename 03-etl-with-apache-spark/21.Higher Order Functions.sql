-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Higher Order Functions
-- MAGIC
-- MAGIC   > - Higher Order Functions are functions that operate on complex data types such as arrays and maps
-- MAGIC   > - They allow you to pass functions as arguments (such as lambda expressions), apply transformations and return arrays or maps 
-- MAGIC   > - They are extremely useful for manipulating arrays without exploding them. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Commonly used Higher Order Array Functions  
-- MAGIC   - TRANSFORM
-- MAGIC   - FILTER
-- MAGIC   - EXISTS
-- MAGIC   - AGGREGATE    

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Syntax
-- MAGIC -----------------------------------------------------------------------------
-- MAGIC `<function_name> (array_column, lambda_expression)` 
-- MAGIC
-- MAGIC _lambda_expression_: `element -> expression`
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW order_items AS
SELECT * FROM 
VALUES
  (1, array('smartphone', 'laptop', 'monitor')),
  (2, array('tablet', 'headphones', 'smartwatch')),
  (3, array('keyboard', 'mouse'))
AS orders(order_id, items);

-- COMMAND ----------

select * from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Convert all the item names to be UPPERCASE (TRANSFORM Function)

-- COMMAND ----------

select order_id,
TRANSFORM(items,x->upper(x)) 
from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Filter only items that contains the string 'smart' (FILTER Function)

-- COMMAND ----------

select order_id,
FILTER(items, x -> x LIKE '%smart%') 
from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Check to see whether the order includes any 'monitor' (EXISTS Function)

-- COMMAND ----------

select order_id,
EXISTS(items,x-> x LIKE '%monitor%') as monitor_existance
from order_items

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Array with more than one object

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW order_items AS
SELECT * FROM VALUES
  (1, array(
        named_struct('name', 'smartphone', 'price', 699),
        named_struct('name', 'laptop', 'price', 1199),
        named_struct('name', 'monitor', 'price', 399)
    )),
  (2, array(
        named_struct('name', 'tablet', 'price', 599),
        named_struct('name', 'headphones', 'price', 199),
        named_struct('name', 'smartwatch', 'price', 299)
    )),
  (3, array(
        named_struct('name', 'keyboard', 'price', 89),
        named_struct('name', 'mouse', 'price', 59)
    ))
AS orders(order_id, items);


-- COMMAND ----------

SELECT * FROM order_items;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Convert all the item names to be UPPERCASE & Add 10% TAX to each item (TRANSFORM Function)

-- COMMAND ----------

select order_id,
TRANSFORM (items,x-> named_struct(
                                  'name',upper(x.name),
                                  'price',round(x.price*1.0,2)))item_with_tax
from order_items

-- COMMAND ----------


