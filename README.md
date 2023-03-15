# pyspark_json_parse

glue data catalog에 등록된 테이블 메타정보(JSON 출력)를 테이블 형태로 파싱하는 작업

### 테이블 리스트 JSON 추출 명령어

```bash
# glue에서 추출한 get-tables json 파일 기반하여 작성
$ aws glue get-tables --database-name ddltest
```

### 대상 JSON 파일 형태

```json
{
    "TableList": [
        {
            "Name": "ddl_test_tab",
            "DatabaseName": "ddltest",
            "CreateTime": "2022-11-24T14:45:14+09:00",
            "UpdateTime": "2023-03-07T09:40:27+09:00",
            "Retention": 0,
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": "seq",
                        "Type": "bigint",
                        "Comment": ""
                    },
                    {
                        "Name": "name",
                        "Type": "varchar(100)",
                        "Comment": ""
                    },
                    {
                        "Name": "age",
                        "Type": "int",
                        "Comment": ""
                    },
                    {
                        "Name": "create_dtm",
                        "Type": "timestamp",
                        "Comment": ""
                    },
                    {
                        "Name": "delete_dtm",
                        "Type": "timestamp",
                        "Comment": ""
                    },
                    {
                        "Name": "string_col",
                        "Type": "string",
                        "Comment": ""
                    }
                ],
                "Location": "s3://jdbc-athena-test/test/DDL_TEST_TAB",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": false,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    }
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {},
                "SkewedInfo": {
                    "SkewedColumnNames": [],
                    "SkewedColumnValues": [],
                    "SkewedColumnValueLocationMaps": {}
                },
                "StoredAsSubDirectories": false
            },
            "PartitionKeys": [],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "parquet.compress": "SNAPPY",
                "transient_lastDdlTime": "1669268714"
            },
            "CreatedBy": "arn:aws:iam::533364636158:user/SDQ-test",
            "IsRegisteredWithLakeFormation": false,
            "CatalogId": "533364636158",
            "VersionId": "1"
        }
    ]
}
```

### parsing 되어야 할 테이블, 컬럼 format

- 테이블

![Untitled (1)](https://user-images.githubusercontent.com/53298013/223743396-6a90af8f-e7d6-423e-9ae1-6745c6efcb8d.png)

- 컬럼

![Untitled (2)](https://user-images.githubusercontent.com/53298013/223743446-ba7a7f5b-c6b2-41a8-8afa-9dfc7a3e0940.png)


