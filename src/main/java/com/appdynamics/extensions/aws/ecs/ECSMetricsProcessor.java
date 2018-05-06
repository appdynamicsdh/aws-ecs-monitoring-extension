/*
 *   Copyright 2018. AppDynamics LLC and its affiliates.
 *   All Rights Reserved.
 *   This is unpublished proprietary source code of AppDynamics LLC and its affiliates.
 *   The copyright notice above does not evidence any actual or intended publication of such source code.
 *
 */

package com.appdynamics.extensions.aws.ecs;

import static com.appdynamics.extensions.aws.Constants.METRIC_PATH_SEPARATOR;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.DimensionFilter;
import com.amazonaws.services.cloudwatch.model.Metric;
import com.amazonaws.services.ecs.AmazonECS;
import com.amazonaws.services.ecs.AmazonECSClient;
import com.amazonaws.services.ecs.model.*;
import com.amazonaws.services.ecs.model.transform.DescribeContainerInstancesResultJsonUnmarshaller;
import com.appdynamics.extensions.aws.config.Account;
import com.appdynamics.extensions.aws.config.Configuration;
import com.appdynamics.extensions.aws.config.MetricType;
import com.appdynamics.extensions.aws.metric.AccountMetricStatistics;
import com.appdynamics.extensions.aws.metric.MetricStatistic;
import com.appdynamics.extensions.aws.metric.NamespaceMetricStatistics;
import com.appdynamics.extensions.aws.metric.RegionMetricStatistics;
import com.appdynamics.extensions.aws.metric.StatisticType;
import com.appdynamics.extensions.aws.metric.processors.MetricsProcessor;
import com.appdynamics.extensions.aws.metric.processors.MetricsProcessorHelper;
import com.appdynamics.extensions.aws.util.AWSUtil;
import com.appdynamics.extensions.yml.YmlReader;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Florencio Sarmiento
 */
public class ECSMetricsProcessor implements MetricsProcessor {

    private static final String NAMESPACE = "AWS/ECS";

    private List<MetricType> metricTypes;

    HashMap<String,String> test = new HashMap<String, String>();

    private Pattern excludeMetricsPattern;

    public ECSMetricsProcessor(List<MetricType> metricTypes,
                               Set<String> excludeMetrics) {
        this.metricTypes = metricTypes;
        this.excludeMetricsPattern = MetricsProcessorHelper.createPattern(excludeMetrics);
    }


    AmazonECSClient getAmazonECSClient(String awsAccessKey, String awsSecretKey, String region) {
        final AmazonECSClient client;

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKey, awsSecretKey);

        client = new AmazonECSClient(awsCreds, clientConfiguration);
        client.setEndpoint("ecs."+region+".amazonaws.com");
        return client;
    }


    public List<Metric> getMetrics(AmazonCloudWatch awsCloudWatch, String accountName) {
        List<Metric> clusterMetrics = MetricsProcessorHelper.getFilteredMetrics(awsCloudWatch,
                NAMESPACE,
                excludeMetricsPattern,
                "ClusterName");

        List<Metric> serviceMetrics = MetricsProcessorHelper.getFilteredMetrics(awsCloudWatch,
                NAMESPACE,
                excludeMetricsPattern,
                "ClusterName", "ServiceName");

        clusterMetrics.addAll(serviceMetrics);

        String configFilename = AWSUtil.resolvePath("monitors/AWSECSMonitor/conf/config.yaml");
        Configuration config = YmlReader.readFromFile(configFilename, Configuration.class);

        for(Account account: config.getAccounts()) {
            for (String region : account.getRegions()) {
                AmazonECS ecs = getAmazonECSClient(account.getAwsAccessKey(), account.getAwsSecretKey(),region);
                ListClustersResult listClustersResult = ecs.listClusters();
                for (String clusterArn : listClustersResult.getClusterArns()) {
                    String cluster = clusterArn.split("/")[1];
                    ListContainerInstancesRequest containerRequest = new ListContainerInstancesRequest().withCluster(cluster);
                    ListContainerInstancesResult listContainerInstancesResult = ecs.listContainerInstances(containerRequest);

                    DescribeContainerInstancesRequest request = new DescribeContainerInstancesRequest().withCluster(cluster).withContainerInstances(listContainerInstancesResult.getContainerInstanceArns());
                    DescribeContainerInstancesResult response = ecs.describeContainerInstances(request);

                    List<ContainerInstance> arns = response.getContainerInstances();

                    for (ContainerInstance containerInstance : arns) {
                        String instanceId = containerInstance.getEc2InstanceId();
                        if (!test.containsKey(instanceId)) {
                            List<DimensionFilter> dimensions = new ArrayList<DimensionFilter>();

                            DimensionFilter dimensionFilter = new DimensionFilter();
                            dimensionFilter.withName("InstanceId");
                            dimensionFilter.withValue(instanceId);
                            dimensions.add(dimensionFilter);

                            clusterMetrics.addAll(MetricsProcessorHelper.getFilteredMetrics(awsCloudWatch,
                                    "AWS/EC2",
                                    excludeMetricsPattern,
                                    dimensions));

                            test.put(instanceId, cluster);
                        }
                    }
                }
            }
        }



        return clusterMetrics;
    }

    public StatisticType getStatisticType(Metric metric) {
        return MetricsProcessorHelper.getStatisticType(metric, metricTypes);
    }

    public Map<String, Double> createMetricStatsMapForUpload(NamespaceMetricStatistics namespaceMetricStats) {
        Map<String, Double> statsMap = new HashMap<String, Double>();
        List<String> clusterNames = new ArrayList<String>();


        if (namespaceMetricStats != null) {
            for (AccountMetricStatistics accountStats : namespaceMetricStats.getAccountMetricStatisticsList()) {
                for (RegionMetricStatistics regionStats : accountStats.getRegionMetricStatisticsList()) {
                    for (MetricStatistic metricStat : regionStats.getMetricStatisticsList()) {
                        String metricPath = createMetricPath(accountStats.getAccountName(),
                                regionStats.getRegion(), metricStat);
                        statsMap.put(metricPath, metricStat.getValue());

                        if(metricStat.getMetric().getDimensions().size()==1) {
                            clusterNames.add(metricStat.getMetric().getDimensions().get(0).getValue());
                        }
                    }
                }
            }
        }




        return statsMap;
    }

    public String getNamespace() {
        return NAMESPACE;
    }

    private String createMetricPath(String accountName, String region,
                                    MetricStatistic metricStatistic) {


        String clusterName = metricStatistic.getMetric().getDimensions().get(0).getValue();
        StringBuilder metricPath;

        if(test.containsKey(clusterName)){
            metricPath = new StringBuilder(accountName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(region)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(test.get(clusterName))
                    .append(METRIC_PATH_SEPARATOR)
                    .append("Instances")
                    .append(METRIC_PATH_SEPARATOR)
                    .append(clusterName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(metricStatistic.getMetric().getMetricName());
        }
        else if(metricStatistic.getMetric().getDimensions().size()>1){
            metricPath = new StringBuilder(accountName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(region)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(metricStatistic.getMetric().getDimensions().get(1).getValue())
                    .append(METRIC_PATH_SEPARATOR)
                    .append("Tasks")
                    .append(METRIC_PATH_SEPARATOR)
                    .append(clusterName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(metricStatistic.getMetric().getMetricName());
        }else{
            metricPath = new StringBuilder(accountName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(region)
                    .append(METRIC_PATH_SEPARATOR)
                    .append(clusterName)
                    .append(METRIC_PATH_SEPARATOR)
                    .append("Cluster Metrics")
                    .append(METRIC_PATH_SEPARATOR)
                    .append(metricStatistic.getMetric().getMetricName());
        }

        return metricPath.toString();
    }

}
