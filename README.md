# AWS ECS Monitoring Extension

## Use Case
Captures ECS metrics and specific instance statistics from Amazon CloudWatch and displays them in the AppDynamics Metric Browser.

**Note : By default, the Machine agent can only send a fixed number of metrics to the controller. This extension potentially reports thousands of metrics, so to change this limit, please follow the instructions mentioned [here](https://docs.appdynamics.com/display/PRO40/Metrics+Limits).**

## Installation

1. Run 'mvn clean install' from aws-ecs-monitoring-extension (skip this step if you want to use the version in the root of this repository)
2. Copy and unzip AWSECSMonitor-\<version\>.zip from 'target' directory (or use the version in the root of this repository) into \<machine_agent_dir\>/monitors/
3. Edit config.yaml file in AWSECSMonitor/conf and provide the required configuration (see Configuration section)
4. Restart the Machine Agent.

## Configuration

### config.yaml

**Note: Please avoid using tab (\t) when editing yaml files. You may want to validate the yaml file using a [yaml validator](http://yamllint.com/).**

| Section | Fields | Description | Example |
| ----- | ----- | ----- | ----- |
| **accounts** | | Fields under this section can be repeated for multiple accounts config |  |
| | awsAccessKey | AWS Access Key, keep it empty if using instance profile |  |
| | awsSecretKey | AWS Secret Key, keep it empty if using instance profile |  |
| | displayAccountName | Display name used in metric path | "MyAWSECS" |
| | regions | Regions where ecs is registered | **Allowed values:**<br/>"ap-southeast-1",<br/>"ap-southeast-2",<br/>"ap-northeast-1",<br/>"eu-central-1",<br/>"eu-west-1",<br/>"us-east-1",<br/>"us-west-1",<br/>"us-west-2",<br/>"sa-east-1" |
| **proxyConfig** | ----- | ----- | ----- |
| | host | The proxy host (must also specify port) |  |
| | port | The proxy port (must also specify host) |  |
| | username | The proxy username (optional)  |  |
| | password | The proxy password (optional)  |  |
| **metricsConfig** | ----- | ----- | ----- |
| metricTypes | | Fields under this section can be repeated for multiple metric types override |  |
| | metricName | The metric name | "CPUUtilization" |
| | statType | The statistic type | **Allowed values:**<br/>"ave"<br/>"max"<br/>"min"<br/>"sum"<br/>"samplecount" |
| | ----- | ----- | ----- |
| | excludeMetrics | Metrics to exclude - supports regex | "CPUUtilization",<br/>"Swap.*" |
| metricsTimeRange |  |  |  |
| | startTimeInMinsBeforeNow | The no of mins to deduct from current time for start time of query | 5 |
| | endTimeInMinsBeforeNow | The no of mins to deduct from current time for end time of query.<br>Note, this must be less than startTimeInMinsBeforeNow | 0 |
| | ----- | ----- | ----- |
| | maxErrorRetrySize | The max number of retry attempts for failed retryable requests | 1 |
| **concurrencyConfig** |  |  |  |
| | noOfAccountThreads | The no of threads to process multiple accounts concurrently | 3 |
| | noOfRegionThreadsPerAccount | The no of threads to process multiple regions per account concurrently | 3 |
| | noOfMetricThreadsPerRegion | The no of threads to process multiple metrics per region concurrently | 3 |
| | ----- | ----- | ----- |
| | metricPrefix | The path prefix for viewing metrics in the metric browser. | "Custom Metrics\|Amazon ECS\|" |


**Below is an example config for monitoring multiple accounts and regions:**

~~~
accounts:
  - awsAccessKey: "XXXXXXXX1"
    awsSecretKey: "XXXXXXXXXX1"
    displayAccountName: "TestAccount_1"
    regions: ["us-east-1","us-west-1","us-west-2"]

  - awsAccessKey: "XXXXXXXX2"
    awsSecretKey: "XXXXXXXXXX2"
    displayAccountName: "TestAccount_2"
    regions: ["eu-central-1","eu-west-1"]

credentialsDecryptionConfig:
    enableDecryption: "false"
    decryptionKey:

proxyConfig:
    host:
    port:
    username:
    password:

metricsConfig:
    metricTypes:
      - metricName: "CurrItems"
        statType: "max"

      - metricName: "DecrHits"
        statType: "sum"        

    excludeMetrics: ["DeleteMisses", "Get.*"]

    metricsTimeRange:
      startTimeInMinsBeforeNow: 5
      endTimeInMinsBeforeNow: 0

    maxErrorRetrySize: 0

concurrencyConfig:
  noOfAccountThreads: 3
  noOfRegionThreadsPerAccount: 3
  noOfMetricThreadsPerRegion: 3

metricPrefix: "Custom Metrics|Amazon ECS|"
~~~

## Metrics
Typical metric path: **Application Infrastructure Performance|\<Tier\>|Custom Metrics|Amazon ECS|\<Account Name\>|\<Region\>|Instance|\<instance id or name\>** followed by the metrics defined in the link below:

- [EC2 Metrics](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/ec2-metricscollected.html)
