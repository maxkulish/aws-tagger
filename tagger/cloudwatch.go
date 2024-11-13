package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go/aws"
)

// tagCloudWatchResources tags CloudWatch alarms and dashboards
func (t *AWSResourceTagger) tagCloudWatchResources() {
	client := cloudwatch.NewFromConfig(t.cfg)

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
