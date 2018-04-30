Specification
=====================

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](http://www.ietf.org/rfc/rfc2119.txt).

Example
---------------

```yaml
stack_name: sample-stack
description: this is my app
sam:
  code_bucket: mizutani-test
  code_prefix: functions

backend:
  sns_topics:
    - name: SecLogUplaod
      arn: arn:aws:sns:ap-northeast-1:1234xxxxxx:seclog-event

handler:
  path: src/handler.py
  args:
    your_key1: value1
    your_key2: value2

bucket_mapping:
  mizutani-test:
    - prefix: logs/azure_ad/signinEvents/
      format: [s3-lines, json, azure-ad-event]
    - prefix: logs/g_suite/
      format: [s3-lines, json, g-suite-login]
```

Root Properties
-----------------

Example.

```yaml
stack_name: sample-stack
description: this is my app
```

| Property Name | Type   | Description                                                    |
|:--------------|:------:|:---------------------------------------------------------------|
| stack_name    | String | **Required**. This will be used for CloudFormation stack name. |
| description   | String | Optional. Description of your serverless application.          |



`sam` Section
-----------------------

**Required**. Properties regarding SAM are defined in this section.

Example:
```
sam:
  code_bucket: mizutani-test
  code_prefix: functions
```

| Property Name | Type   | Description                                       |
|:--------------|:------:|:--------------------------------------------------|
| code_bucket   | String | **Required**. S3 bucket to store a code zip file. |
| code_prefix   | String | **Required**. S3 key prefix of a code zip file.   |


`backend` Section
-------------------

### Top Level Properties

| Property Name           | Type        | Description                                          |
|:------------------------|:-----------:|:-----------------------------------------------------|
| kinesis_stream_fast_arn | String(ARN) | Optional. Kinesis stream for fast lane               |
| kinesis_stream_slow_arn | String(ARN) | Optional. Kinesis stream for slow lane               |
| dynamodb_arn            | String(ARN) | Optional. DynamoDB table for errored task management |
| dlq_sns_arn             | String(ARN) | Optional. SNS topic for Dead Letter Queue            |




### `role_arn` Subsection

Optional. If the IAM role(s) is not defiend, SLIPS create appropriate IAM role automatically in CloudFormation deployment phase.

| Property Name | Type        | Description                               |
|:--------------|:-----------:|:------------------------------------------|
| event_pusher  | String(ARN) | Optional. ARN of IAM role for EventPusher |
| dispatcher    | String(ARN) | Optional. ARN of IAM role for Dispathcer  |
| reporter      | String(ARN) | Optional. ARN of IAM role for Reporter    |
| drain         | String(ARN) | Optional. ARN of IAM role for Drain       |

### `sns_topic` Subsection

**Required**. List of object. Properties of an object are following.

| Property Name | Type        | Description                                                            |
|:--------------|:-----------:|:-----------------------------------------------------------------------|
| name          | String      | **Required**. Unique name of SNS event sournce.                        |
| arn           | String(ARN) | **Required**. ARN of existing SNS topic to send S3 ObjectCreated event |


### Example

```
backend:
  role_arn:
    reporter:     arn:aws:iam::12345xxx:role/MyReporterRole
    drain:        arn:aws:iam::12345xxx:role/MyDrainRole
  dynamodb_arn:   arn:aws:dynamodb:ap-northeast-1:12345xxx:table/MyTable
  dlq_sns_arn:    arn:aws:sns:ap-northeast-1:12345xxx:MyDLQ

  sns_topics:
    - name: SecLogUplaod
      arn: arn:aws:sns:ap-northeast-1:12345xxx:seclog-event
```

In this case, IAM roles for EventPusher and Dispatcher will be created. `arn:aws:iam::12345xxx:role/My{Reporter,Drain}Role` will be assigned to Reporter and Drain as IAM role. In the same manner, existing DynamoDB and DLQ(SNS) resources are used for the serverless application and 2 Kinesis streams (Fast and Slow) are created. Additioanlly, the serverless application is triggered by SNS topic `seclog-event`.


`handler` Section
-----------------

**Required**. This section desribes handler that means your own code. 

| Property Name | Type        | Description                                                         |
|:--------------|:-----------:|:--------------------------------------------------------------------|
| role_arn      | String(ARN) | Optional. IAM role for MainFunc.                                    |
| path          | String      | **Required**. Path of a source file including your function.        |
| args          | Object      | Optional. The structure data that you want to pass to your function |


### Example

```
handler:
  role_arn: arn:aws:iam::214219211678:role/LambdaMizutaniSlamMain
  path: handler/readonly.py
  args:
    test_value: A
```

`bucket_mapping` Section
--------------------

**Required**. This section describes mappings of S3 bucket name + key and format of objects.

```
bucket_mapping:
  slips-test
    - prefix: logs/azure-ad/audit/
      format: [s3-lines, json, azure-ad-audit]
    - prefix: logs/cylance/
      format: [s3-lines, json, cylance]
```



