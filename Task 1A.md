# Assignment

1. Create a dummy data generator code in Python that creates a realtime stream of random numbers between 50 to 100 every 2 seconds 

2. Create an Event Hub that captures the real time stream 

3. Capture the incoming event hubs stream using Structured Streaming in Databricks 

4. Add a column '`Risk`' to the stream where if `value > 80` then risk will be '`High`' else '`Low`' 

5. Capture the output stream in a seperate Event Hub 

6. Connect the output stream to Power BI using Stream Analytics 

7. Build a realtime dashboard in Power BI that counts number of High and Low risk values.

# Solution

## Step 1 : Dummy data generator every 2 seconds

Reference : https://stackoverflow.com/questions/24204582/generate-multiple-independent-random-streams-in-python

Stream meaning continuity of data every 2 seconds

```py
import random
import time

def generate_random_numbers():
  while True:
    random_number = random.randint(50, 100)
    print(random_number)
    time.sleep(2)

if __name__ == "__main__":
  generate_random_numbers()
```

Sample Output
```
45
<2 second sleep>
56
<2 second sleep>
42
...
...
```

## Step 2 : Creating event hub to capture real time stream

<span style="color:red"> Could not test </span>


Read [microsoft blog](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview).
Steps as suggested by Gemini:
1. Create an Event Hubs Namespace
    - Log in to the Azure portal.
    - Click on "Create a resource".
    - Search for "Event Hubs" and select it.
    - Choose a subscription, resource group, and location.
    - Enter a unique namespace name.    
    - Select a pricing tier (Standard for capture feature).
    - Click "Create".
2. Create an Event Hub
    - Navigate to the created Event Hubs namespace.
    - Click on "+ Event Hub".
    - Enter a name for your event hub.
    - Select the partition count (default is 4).
    - Enable "Capture" and choose a storage account for capturing events.
    - Select the capture file format (e.g., Avro, Parquet).
    - Configure capture interval and file size.
    - Click "Create".
3. Sending Data to Event Hub
    - Use the Azure Event Hubs SDK for your preferred language to send data to the event hub.
```py
import azure.eventhub
from azure.eventhub.aio import EventHubProducerClient

def send_data_to_event_hub(event_hub_connection_string, data):
    producer = EventHubProducerClient.from_connection_string(event_hub_connection_string)
    async with producer:
        event_data = azure.eventhub.EventData(data.encode('utf-8'))
        await producer.send_batch([event_data])

# Replace with your event hub connection string
connection_str = "YOUR_EVENT_HUB_CONNECTION_STRING"

while True:
    random_number = random.randint(50, 100)
    send_data_to_event_hub(connection_str, str(random_number))
    time.sleep(2)
```

## Step 3 : Capture the incoming event hubs stream

<span style="color:red"> Could not test </span>


Read [microsoft blogs](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview)
As per Gemini suggestion
```py
'''-----import libraries and generate spark session------'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType

spark = SparkSession.builder \
    .appName("EventHubStructuredStreaming") \
    .getOrCreate()

'''--------Define Event Hub Parameters--------'''
event_hubs_namespace = "your_event_hubs_namespace"
event_hub_name = "your_event_hub_name"
eventhubs_conn_str = "Endpoint=sb://your_namespace.servicebus.windows.net/;EntityPath=your_event_hub_name;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your_access_key"


'''------------Create Schema for Incoming Data-------------'''
schema = StructType([
    StructField("value", IntegerType(), True)
])

'''---------Read from Event Hub----------'''
df = spark \
  .readStream \
  .format("eventhubs") \
  .option("eventhubs.connectionString", eventhubs_conn_str) \
  .option("eventhubs.consumerGroup", "$Default") \
  .option("startingOffsets", "earliest") \
  .load()

# Cast the value column to integer
df = df.withColumn("value", col("value").cast("integer"))

'''-------Process the Stream--------'''
# Example: Calculate average value
average_value = df.select(avg("value")).groupBy().count()

# Write to console
average_value.writeStream \
  .outputMode("Complete") \
  .format("console") \
  .start()
```
## Step 4 : Add a column 'Risk' to the stream where if value > 80 then risk will be 'High' else 'Low'


```py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType

# ... (rest of your code)

# Create a schema for incoming data
schema = StructType([
    StructField("value", IntegerType(), True)
])

# Read from Event Hub
df = spark \
  .readStream \
  .format("eventhubs") \
  .option("eventhubs.connectionString", eventhubs_conn_str) \
  .option("eventhubs.consumerGroup", "$Default") \
  .option("startingOffsets", "earliest") \
  .load()

# Cast the value column to integer
df = df.withColumn("value", col("value").cast("integer"))

# Add the 'Risk' column based on the 'value' column
df = df.withColumn("Risk", when(col("value") > 80, "High").otherwise("Low"))

```

## Step 5 : Capture the output stream in a seperate Event Hub

```py
# ... (rest of your code)

# Write output to a separate Event Hub
output_eventhubs_conn_str = "YOUR_OUTPUT_EVENT_HUB_CONNECTION_STRING"

df.writeStream \
  .format("eventhubs") \
  .option("eventhubs.connectionString", output_eventhubs_conn_str) \
  .option("eventhubs.consumerGroup", "$Default") \
  .outputMode("Append") \
  .start()
```

## Step 6 : Connect the output stream to Power BI using Stream Analytics

From Gemini analysed the approach

#### 1. Create an Azure Stream Analytics Job:
* Navigate to the Azure portal and create a new Stream Analytics job.
* Configure the input to point to your Event Hub.
* Define the output to be Power BI.
* Specify the query to process the incoming data, including the logic to add the 'Risk' column.

#### 2. Configure Power BI Output:
* In the Stream Analytics job, under the output configuration, select Power BI as the output type.
* Authorize the connection to your Power BI workspace.
* Specify the dataset and table name where the data will be pushed.

#### 3. Create a Power BI Dataset:
* In Power BI, create a new dataset.
* Ensure the dataset name matches the one specified in the Stream Analytics job.

#### 4. Create Visualizations:
* Once data starts flowing into Power BI, create visualizations based on the incoming data.

### Code Example (Stream Analytics Query):
```sql
WITH EnrichedData AS (
  SELECT
      *,
      CASE WHEN value > 80 THEN 'High' ELSE 'Low' END AS Risk
  FROM Input
)
SELECT
    *
INTO
    [PowerBIOutput]
FROM EnrichedData;
```

## Step 7 : Build a realtime dashboard in Power BI that counts number of High and Low risk values.


### Creating a Power BI Dataset
1. **Get Data:** In Power BI Desktop, go to 'Get Data' -> 'More' -> 'Azure' -> 'Azure Stream Analytics'.
2. **Connect to Stream Analytics Job:** Provide the necessary credentials and select the output dataset from your Stream Analytics job.
3. **Load Data:** Load the data into Power BI.

### Creating Measures
1. **Create a measure for High Risk Count:**
   ```dax
   High Risk Count = COUNTROWS(FILTER(YourTableName, YourTableName[Risk] = "High"))
   ```
2. **Create a measure for Low Risk Count:**
   ```dax
   Low Risk Count = COUNTROWS(FILTER(YourTableName, YourTableName[Risk] = "Low"))
   ```
   Replace `YourTableName` with the actual name of your table in Power BI.

### Creating a Dashboard
1. Create a new dashboard in Power BI.
2. Add two card visuals to the dashboard.
3. Assign the `High Risk Count` and `Low Risk Count` measures to the respective cards.
4. Format the cards to display the counts clearly.

