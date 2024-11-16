package tagger

import (
	"context"
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

// tagCloudWatchResources creates a CloudWatch client and initiates the tagging process
func (t *AWSResourceTagger) tagCloudWatchResources() {
	client := cloudwatch.NewFromConfig(t.cfg)
	t.tagCloudWatchResourcesWithClient(client)
}

// tagCloudWatchResourcesWithClient tags CloudWatch alarms and dashboards with the provided client.
// It logs the process and handles errors. The process includes pagination for fetching alarms and dashboards.
func (t *AWSResourceTagger) tagCloudWatchResourcesWithClient(client CloudWatchAPI) {
	log.Println("Starting CloudWatch resource tagging...")
	defer log.Println("Completed CloudWatch resource tagging")

	if len(t.tags) == 0 {
		log.Println("No tags provided, skipping CloudWatch resource tagging")
		return
	}

	var (
		totalAlarms      int
		taggedAlarms     int
		failedAlarms     int
		totalDashboards  int
		taggedDashboards int
		failedDashboards int
	)

	// Tag CloudWatch Alarms with pagination
	log.Println("Discovering CloudWatch alarms...")
	var nextTokenAlarms *string
	for {
		output, err := client.DescribeAlarms(t.ctx, &cloudwatch.DescribeAlarmsInput{
			NextToken: nextTokenAlarms,
		})
		if err != nil {
			log.Printf("Error describing CloudWatch alarms: %v", err)
			break
		}

		totalAlarms += len(output.MetricAlarms)
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
			log.Printf("Successfully tagged CloudWatch alarm: %s", *alarm.AlarmName)
		}

		if output.NextToken == nil {
			break
		}
		nextTokenAlarms = output.NextToken
	}

	// Tag CloudWatch Dashboards with pagination
	log.Println("Discovering CloudWatch dashboards...")
	var nextTokenDashboards *string
	for {
		dashboards, err := client.ListDashboards(t.ctx, &cloudwatch.ListDashboardsInput{
			NextToken: nextTokenDashboards,
		})
		if err != nil {
			log.Printf("Error listing CloudWatch dashboards: %v", err)
			break
		}

		totalDashboards += len(dashboards.DashboardEntries)
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
			log.Printf("Successfully tagged CloudWatch dashboard: %s", *dashboard.DashboardName)
		}

		if dashboards.NextToken == nil {
			break
		}
		nextTokenDashboards = dashboards.NextToken
	}

	// Print summary
	log.Println("CloudWatch Tagging Summary:")
	log.Printf("Alarms: Total=%d, Tagged=%d, Failed=%d", totalAlarms, taggedAlarms, failedAlarms)
	log.Printf("Dashboards: Total=%d, Tagged=%d, Failed=%d", totalDashboards, taggedDashboards, failedDashboards)
}
