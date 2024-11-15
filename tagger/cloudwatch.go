package tagger

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go/aws"
)

// CloudWatchAPI interface for CloudWatch client operations
type CloudWatchAPI interface {
	DescribeAlarms(ctx context.Context, params *cloudwatch.DescribeAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error)
	ListDashboards(ctx context.Context, params *cloudwatch.ListDashboardsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListDashboardsOutput, error)
	TagResource(ctx context.Context, params *cloudwatch.TagResourceInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.TagResourceOutput, error)
}

// Add this method to tagCloudWatchResources in cloudwatch.go
func (t *AWSResourceTagger) tagCloudWatchResourcesWithClient(client CloudWatchAPI) {
	log.Println("Starting CloudWatch resource tagging...")

	// Counters for logging
	var (
		totalAlarms      int
		taggedAlarms     int
		failedAlarms     int
		totalDashboards  int
		taggedDashboards int
		failedDashboards int
	)

	// Tag CloudWatch Alarms
	log.Println("Discovering CloudWatch alarms...")
	output, err := client.DescribeAlarms(t.ctx, &cloudwatch.DescribeAlarmsInput{})
	if err != nil {
		log.Printf("Error describing CloudWatch alarms: %v", err)
	} else {
		totalAlarms = len(output.MetricAlarms)
		log.Printf("Found %d CloudWatch alarms", totalAlarms)

		for _, alarm := range output.MetricAlarms {
			cwTags := make([]cloudwatchtypes.Tag, 0, len(t.tags))
			for k, v := range t.tags {
				cwTags = append(cwTags, cloudwatchtypes.Tag{
					Key:   aws.String(k),
					Value: aws.String(v),
				})
			}

			_, err := client.TagResource(t.ctx, &cloudwatch.TagResourceInput{
				ResourceARN: alarm.AlarmArn,
				Tags:        cwTags,
			})
			if err != nil {
				failedAlarms++
				t.handleError(err, *alarm.AlarmArn, "CloudWatch Alarm")
				continue
			}
			taggedAlarms++
		}
	}

	// Tag CloudWatch Dashboards
	log.Println("Discovering CloudWatch dashboards...")
	dashboards, err := client.ListDashboards(t.ctx, &cloudwatch.ListDashboardsInput{})
	if err != nil {
		log.Printf("Error listing CloudWatch dashboards: %v", err)
	} else {
		totalDashboards = len(dashboards.DashboardEntries)
		log.Printf("Found %d CloudWatch dashboards", totalDashboards)

		for _, dashboard := range dashboards.DashboardEntries {
			cwTags := make([]cloudwatchtypes.Tag, 0, len(t.tags))
			for k, v := range t.tags {
				cwTags = append(cwTags, cloudwatchtypes.Tag{
					Key:   aws.String(k),
					Value: aws.String(v),
				})
			}

			_, err := client.TagResource(t.ctx, &cloudwatch.TagResourceInput{
				ResourceARN: dashboard.DashboardArn,
				Tags:        cwTags,
			})
			if err != nil {
				failedDashboards++
				t.handleError(err, *dashboard.DashboardArn, "CloudWatch Dashboard")
				continue
			}
			taggedDashboards++
		}
	}

	// Print summary
	log.Println("CloudWatch Tagging Summary:")
	log.Printf("Alarms: Total=%d, Tagged=%d, Failed=%d", totalAlarms, taggedAlarms, failedAlarms)
	log.Printf("Dashboards: Total=%d, Tagged=%d, Failed=%d", totalDashboards, taggedDashboards, failedDashboards)
}

// tagCloudWatchResources tags CloudWatch alarms and dashboards
func (t *AWSResourceTagger) tagCloudWatchResources() {
	client := cloudwatch.NewFromConfig(t.cfg)
	t.tagCloudWatchResourcesWithClient(client)

	log.Println("Starting CloudWatch resource tagging...")

	// Counters for logging
	var (
		totalAlarms      int
		taggedAlarms     int
		failedAlarms     int
		totalDashboards  int
		taggedDashboards int
		failedDashboards int
	)

	// Tag CloudWatch Alarms
	log.Println("Discovering CloudWatch alarms...")
	alarmPaginator := cloudwatch.NewDescribeAlarmsPaginator(client, &cloudwatch.DescribeAlarmsInput{})
	for alarmPaginator.HasMorePages() {
		page, err := alarmPaginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error describing CloudWatch alarms: %v", err)
			continue
		}

		totalAlarms += len(page.MetricAlarms)
		log.Printf("Found %d CloudWatch alarms in current page", len(page.MetricAlarms))

		for _, alarm := range page.MetricAlarms {
			log.Printf("Processing alarm: %s (ARN: %s)", *alarm.AlarmName, *alarm.AlarmArn)

			// Convert tags to CloudWatch tag format
			cwTags := make([]cloudwatchtypes.Tag, 0, len(t.tags))
			for k, v := range t.tags {
				cwTags = append(cwTags, cloudwatchtypes.Tag{
					Key:   aws.String(k),
					Value: aws.String(v),
				})
			}

			_, err := client.TagResource(t.ctx, &cloudwatch.TagResourceInput{
				ResourceARN: alarm.AlarmArn,
				Tags:        cwTags,
			})
			if err != nil {
				failedAlarms++
				t.handleError(err, *alarm.AlarmArn, "CloudWatch Alarm")
				continue
			}
			taggedAlarms++
			log.Printf("Successfully tagged CloudWatch alarm: %s with tags: %v", *alarm.AlarmName, t.tags)
		}
	}

	// Tag CloudWatch Dashboards
	log.Println("\nDiscovering CloudWatch dashboards...")
	dashboardPaginator := cloudwatch.NewListDashboardsPaginator(client, &cloudwatch.ListDashboardsInput{})
	for dashboardPaginator.HasMorePages() {
		page, err := dashboardPaginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error listing CloudWatch dashboards: %v", err)
			continue
		}

		totalDashboards += len(page.DashboardEntries)
		log.Printf("Found %d CloudWatch dashboards in current page", len(page.DashboardEntries))

		for _, dashboard := range page.DashboardEntries {
			log.Printf("Processing dashboard: %s (ARN: %s)", *dashboard.DashboardName, *dashboard.DashboardArn)

			cwTags := make([]cloudwatchtypes.Tag, 0, len(t.tags))
			for k, v := range t.tags {
				cwTags = append(cwTags, cloudwatchtypes.Tag{
					Key:   aws.String(k),
					Value: aws.String(v),
				})
			}

			_, err := client.TagResource(t.ctx, &cloudwatch.TagResourceInput{
				ResourceARN: dashboard.DashboardArn,
				Tags:        cwTags,
			})
			if err != nil {
				failedDashboards++
				t.handleError(err, *dashboard.DashboardArn, "CloudWatch Dashboard")
				continue
			}
			taggedDashboards++
			log.Printf("[+] Successfully tagged CloudWatch dashboard: %s with tags: %v", *dashboard.DashboardName, t.tags)
		}
	}

	// Print summary
	fmt.Println("=====================================")
	fmt.Println("CloudWatch Tagging Summary:")
	log.Printf("Alarms:")
	log.Printf("  - Total discovered: %d", totalAlarms)
	log.Printf("  - Successfully tagged: %d", taggedAlarms)
	log.Printf("  - Failed to tag: %d", failedAlarms)
	log.Printf("Dashboards:")
	log.Printf("  - Total discovered: %d", totalDashboards)
	log.Printf("  - Successfully tagged: %d", taggedDashboards)
	log.Printf("  - Failed to tag: %d", failedDashboards)
	log.Println("Completed CloudWatch resource tagging")
}
