package tagger

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// GlueMetrics struct extension
type GlueMetrics struct {
	DatabasesFound    int32
	DatabasesTagged   int32
	DatabasesFailed   int32
	ConnectionsFound  int32
	ConnectionsTagged int32
	ConnectionsFailed int32
	JobsFound         int32
	JobsTagged        int32
	JobsFailed        int32
	CrawlersFound     int32
	CrawlersTagged    int32
	CrawlersFailed    int32
}

// tagGlueResources tags AWS Glue resources
func (t *AWSResourceTagger) tagGlueResources() {
	log.Println("Tagging Glue resources...")
	client := glue.NewFromConfig(t.cfg)

	metrics := &GlueMetrics{}

	// Tag all supported Glue resource types
	//t.tagGlueDatabases(client, metrics)
	//t.tagGlueConnections(client, metrics)
	t.tagGlueCrawlers(client, metrics)
	//t.tagGlueJobs(client, metrics)
	//t.tagGlueTriggers(client)
	//t.tagGlueWorkflows(client)
	//t.tagGlueBlueprints(client)
	//t.tagGlueMLTransforms(client)
	//t.tagGlueDataQualityRulesets(client)
	//t.tagGlueSchemaRegistries(client)
	//t.tagGlueSchemas(client)
	//t.tagGlueDevEndpoints(client)
	//t.tagGlueInteractiveSessions(client)

	log.Println("Completed tagging Glue resources")
}

// tagGlueDatabases tags Glue databases (skipping tables since they're not taggable)
func (t *AWSResourceTagger) tagGlueDatabases(client *glue.Client, metrics *GlueMetrics) {
	databases, err := client.GetDatabases(t.ctx, &glue.GetDatabasesInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Databases")
		return
	}

	atomic.StoreInt32(&metrics.DatabasesFound, int32(len(databases.DatabaseList)))
	log.Printf("Found %d Glue databases to tag", metrics.DatabasesFound)

	for _, db := range databases.DatabaseList {
		dbName := aws.ToString(db.Name)
		if err := t.tagDatabase(client, dbName); err != nil {
			log.Printf("Error processing database %s: %v", dbName, err)
			continue
		}
		atomic.AddInt32(&metrics.DatabasesTagged, 1)
	}

	log.Printf("Databases: Found: %d, Tagged: %d, Failed: %d",
		metrics.DatabasesFound, metrics.DatabasesTagged, metrics.DatabasesFailed)
}

// tagDatabase tags a single Glue database
func (t *AWSResourceTagger) tagDatabase(client *glue.Client, dbName string) error {
	resourceArn := t.buildCompoundARN(GlueDatabase, dbName)
	log.Printf("database ARN: %s", resourceArn)

	_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagsToAdd:   t.convertToGlueTags(),
	})
	if err != nil {
		return fmt.Errorf("error tagging database %s: %w", dbName, err)
	}

	log.Printf("Successfully tagged Glue database: %s", dbName)
	return nil
}

// convertToGlueTags converts the common tags map to Glue-specific tags
func (t *AWSResourceTagger) convertToGlueTags() map[string]string {
	return t.tags
}

// tagGlueConnections tags AWS Glue connections with metrics
func (t *AWSResourceTagger) tagGlueConnections(client *glue.Client, metrics *GlueMetrics) {
	log.Println("Tagging Glue connections...")

	connections, err := client.GetConnections(t.ctx, &glue.GetConnectionsInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Connections")
		return
	}

	atomic.StoreInt32(&metrics.ConnectionsFound, int32(len(connections.ConnectionList)))
	log.Printf("Found %d Glue connections to tag", metrics.ConnectionsFound)

	for _, conn := range connections.ConnectionList {
		if err := t.tagConnection(client, conn); err != nil {
			log.Printf("Error tagging connection %s: %v", aws.ToString(conn.Name), err)
			atomic.AddInt32(&metrics.ConnectionsFailed, 1)
			continue
		}
		atomic.AddInt32(&metrics.ConnectionsTagged, 1)
	}

	log.Printf("Connections: Found: %d, Tagged: %d, Failed: %d",
		metrics.ConnectionsFound, metrics.ConnectionsTagged, metrics.ConnectionsFailed)
}

// tagConnection tags a single Glue connection
func (t *AWSResourceTagger) tagConnection(client *glue.Client, conn gluetypes.Connection) error {
	connName := aws.ToString(conn.Name)

	// Build connection ARN using the predefined pattern
	resourceArn := t.buildCompoundARN(GlueConnection, connName)
	log.Printf("Connection ARN: %s", resourceArn)

	// Apply tags
	_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagsToAdd:   t.convertToGlueTags(),
	})
	if err != nil {
		return fmt.Errorf("error tagging connection %s: %w", connName, err)
	}

	log.Printf("Successfully tagged Glue connection: %s", connName)
	return nil
}

// Glue Jobs
// tagGlueJobs tags AWS Glue jobs with metrics
func (t *AWSResourceTagger) tagGlueJobs(client *glue.Client, metrics *GlueMetrics) {
	log.Println("Tagging Glue jobs...")

	// Initialize paging parameters
	maxResults := int32(100)
	var nextToken *string

	for {
		input := &glue.GetJobsInput{
			MaxResults: aws.Int32(maxResults),
			NextToken:  nextToken,
		}

		jobs, err := client.GetJobs(t.ctx, input)
		if err != nil {
			t.handleError(err, "all", "Glue Jobs")
			return
		}

		jobCount := int32(len(jobs.Jobs))
		atomic.AddInt32(&metrics.JobsFound, jobCount)
		log.Printf("Found %d Glue jobs to tag in this batch", jobCount)

		for _, job := range jobs.Jobs {
			if err := t.tagJob(client, job); err != nil {
				log.Printf("Error tagging job %s: %v", aws.ToString(job.Name), err)
				atomic.AddInt32(&metrics.JobsFailed, 1)
				continue
			}
			atomic.AddInt32(&metrics.JobsTagged, 1)
		}

		// Check if there are more jobs to process
		if jobs.NextToken == nil {
			break
		}
		nextToken = jobs.NextToken
	}

	log.Printf("Completed tagging Glue jobs. Found: %d, Tagged: %d, Failed: %d",
		metrics.JobsFound, metrics.JobsTagged, metrics.JobsFailed)
}

// tagJob tags a single Glue job
func (t *AWSResourceTagger) tagJob(client *glue.Client, job gluetypes.Job) error {
	jobName := aws.ToString(job.Name)

	// Build job ARN using the predefined pattern
	resourceArn := t.buildCompoundARN(GlueJob, jobName)
	log.Printf("Job ARN: %s", resourceArn)

	// Apply tags
	_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagsToAdd:   t.convertToGlueTags(),
	})
	if err != nil {
		return fmt.Errorf("error tagging job %s: %w", jobName, err)
	}

	log.Printf("Successfully tagged Glue job: %s", jobName)
	return nil
}

// Glue Crawlers
// tagGlueCrawlers tags AWS Glue crawlers with metrics
func (t *AWSResourceTagger) tagGlueCrawlers(client *glue.Client, metrics *GlueMetrics) {
	log.Println("Tagging Glue crawlers...")

	// Initialize paging parameters
	maxResults := int32(100)
	var nextToken *string

	for {
		input := &glue.GetCrawlersInput{
			MaxResults: aws.Int32(maxResults),
			NextToken:  nextToken,
		}

		crawlers, err := client.GetCrawlers(t.ctx, input)
		if err != nil {
			t.handleError(err, "all", "Glue Crawlers")
			return
		}

		crawlerCount := int32(len(crawlers.Crawlers))
		atomic.AddInt32(&metrics.CrawlersFound, crawlerCount)
		log.Printf("Found %d Glue crawlers to tag in this batch", crawlerCount)

		for _, crawler := range crawlers.Crawlers {
			if err := t.tagCrawler(client, crawler); err != nil {
				log.Printf("Error tagging crawler %s: %v", aws.ToString(crawler.Name), err)
				atomic.AddInt32(&metrics.CrawlersFailed, 1)
				continue
			}
			atomic.AddInt32(&metrics.CrawlersTagged, 1)
		}

		// Check if there are more crawlers to process
		if crawlers.NextToken == nil {
			break
		}
		nextToken = crawlers.NextToken
	}

	log.Printf("Completed tagging Glue crawlers. Found: %d, Tagged: %d, Failed: %d",
		metrics.CrawlersFound, metrics.CrawlersTagged, metrics.CrawlersFailed)
}

// tagCrawler tags a single Glue crawler
func (t *AWSResourceTagger) tagCrawler(client *glue.Client, crawler gluetypes.Crawler) error {
	crawlerName := aws.ToString(crawler.Name)

	// Build crawler ARN using the predefined pattern
	resourceArn := t.buildCompoundARN(GlueCrawler, crawlerName)
	log.Printf("Crawler ARN: %s", resourceArn)

	// Apply tags
	_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
		ResourceArn: aws.String(resourceArn),
		TagsToAdd:   t.convertToGlueTags(),
	})
	if err != nil {
		return fmt.Errorf("error tagging crawler %s: %w", crawlerName, err)
	}

	log.Printf("Successfully tagged Glue crawler: %s", crawlerName)
	return nil
}
