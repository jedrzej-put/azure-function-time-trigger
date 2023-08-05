# Loading Wrocław data into SQL DB using Databricks and Queue Storage
<img src="https://github.com/jedrzej-put/azure-function-time-trigger/blob/main/schema.jpg" width="600" height="250"  title="Sample Input Image">

## AF functionality:​
- Get data from open API:​
- https://www.wroclaw.pl/open-data/​
- http://powietrze.gios.gov.pl/pjp/content/api​
- Create containers in Data Lake if not exist​
- Create queues in Queue Storage if not exist​
- Save data as string on Data Lake​
- Send messages to queues​

## Data triggers:​
- Transport data (downloading every 30 seconds)​
- Wrocław weather data (downloading every 30 minutes)​
- Wrocław car parkings data (downloading every 15 minutes)​
- Number of cars arrived to Wrocław (downloading every day at 12:00 AM)​
- Wrocław rains data (downloading every day at 10:00 AM)​
- Wrocław air data (downloading every two days at 11:45 AM)

## Databricks
- Create connection with SQL DB​
- Create connection with Queue Storage (queue client)​
- Check if queue is not empty​
- Convert messages to Spark DataFrame​
- Union messages to one DataFrame​
- Deletem messages from queue​
- Drop duplicated rows from DataFrame​
- Change type of columns in DataFrame​
- Load DataFrame to SQL DB

## local.settings.json

```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": <account_connection_string>,
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "jedrzejsmokstorageaccoun_STORAGE": <account_connection_string>,
    "AzureWebJobsAzureStorageQueuesConnectionString": <account_connection_string>
  }
}
```
