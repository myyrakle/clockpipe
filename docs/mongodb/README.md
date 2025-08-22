# MongoDB Guideline

- MongoDB's CDC is implemented via [Change Stream](https://www.mongodb.com/docs/manual/changeStreams/).
- Change streams are only available in replica sets and partitioned clusters, so they are not available in single-node MongoDB.

## MongoDB Setup

- No additional setup is required.

---

## MongoDB Config

```json
    "source_type": "mongodb",
    "mongodb": {
        "connection": {
            "host": "....mongodb.net",
            "port": 27017,
            "username": "user",
            "password": "q1w2e3r4",
            "database": "dbname"
        },
        "collections": [
            {
                "collection_name": "user",
                "skip_copy": false,
                "mask_fields": ["password"]
            }
        ],
        "copy_batch_size": 1000,
        "resume_token_storage": "file",
        "resume_token_path": "./token.json"
    }
```

| name                          | description                                                   | required | default           |
| :---------------------------- | :------------------------------------------------------------ | :------- | :---------------- |
| copy_batch_size               | Limit on retrieving data at once when doing First Copy.       | false    | 1000              |
| resume_token_storage          | How to record a cursor for CDC                                | false    | file              |
| resume_token_path             | (if file) file path of cursor for CDC                         | false    | resume_token.json |
| connection                    | MongoDB Database Connection Info                              | true     |                   |
| collections                   | collections to sync                                           | true     |                   |
| collections[].collection_name | collection name                                               | true     |                   |
| collections[].mask_columns    | Masks the values ​​of specific columns to default values      | false    |                   |
| collections[].skip_copy       | Skip the first copy during initial synchronization (CDC only) | false    | false             |
