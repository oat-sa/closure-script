# PoC of closure Go script

## Setup

Need add follwing env variables

`REDIS_CLOSURE_DSN` - Redis storage DSN

`REDIS_CLOSURE_NAMESPACE` - Redis namespace

`GOOGLE_APPLICATION_CREDENTIALS` - path JSON keyfile

`GOOGLE_CLOUD_PROJECT` - Google Cloud project

`CLOSURE_TOPIC_ID` - Closure Topic

## Build

```
go build -o closure .
```