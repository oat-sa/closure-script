# PoC of closure Go script

This script fetches data about delivery executions from Closure queue which should be closed at specific time. And put a message to results queue

## Setup

Need add following env variables

`GOOGLE_APPLICATION_CREDENTIALS` - path JSON keyfile

`GOOGLE_CLOUD_PROJECT` - Google Cloud project

`RESULTS_TOPIC_ID` - Results Extraction Topic

`CLOSURE_SUB_ID` - Closure Subscription

`CLOSURE_TOPIC_ID` - Closure Topic

## Build

```
go build -o closure .
```