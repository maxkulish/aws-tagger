package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// tagGlueResources tags AWS Glue resources including databases, tables, jobs, and crawlers
func (t *AWSResourceTagger) tagGlueResources() {
	fmt.Println("=====================================")
	log.Println("Tagging Glue resources...")
	client := glue.NewFromConfig(t.cfg)

	// Tag databases and tables
	t.tagGlueDatabases(client)

	// Tag jobs
	t.tagGlueJobs(client)

	// Tag crawlers
	t.tagGlueCrawlers(client)

	// Tag workflows
	t.tagGlueWorkflows(client)

	// Tag Dev Endpoints
	t.tagGlueDevEndpoints(client)

	// Tag ML Transforms
	t.tagGlueMLTransforms(client)

	log.Println("Completed tagging Glue resources")
}

// tagGlueDatabases tags Glue databases and their tables
func (t *AWSResourceTagger) tagGlueDatabases(client *glue.Client) {
	// Get all databases
	databases, err := client.GetDatabases(t.ctx, &glue.GetDatabasesInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Databases")
		return
	}

	for _, db := range databases.DatabaseList {
		// Tag database
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: db.CatalogId, // Using CatalogId as the ARN for the database
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(db.Name), "Glue Database")
			continue
		}
		log.Printf("Successfully tagged Glue database: %s", aws.ToString(db.Name))

		// Get and tag tables in the database
		tables, err := client.GetTables(t.ctx, &glue.GetTablesInput{
			DatabaseName: db.Name,
		})
		if err != nil {
			t.handleError(err, aws.ToString(db.Name), "Glue Tables")
			continue
		}

		for _, table := range tables.TableList {
			_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
				ResourceArn: table.CatalogId, // Using CatalogId as the ARN for the table
				TagsToAdd:   t.convertToGlueTags(),
			})
			if err != nil {
				t.handleError(err, aws.ToString(table.Name), "Glue Table")
				continue
			}
			log.Printf("Successfully tagged Glue table: %s.%s", aws.ToString(db.Name), aws.ToString(table.Name))
		}
	}
}

// tagGlueJobs tags Glue jobs
func (t *AWSResourceTagger) tagGlueJobs(client *glue.Client) {
	jobs, err := client.GetJobs(t.ctx, &glue.GetJobsInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Jobs")
		return
	}

	for _, job := range jobs.Jobs {
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: job.Name,
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(job.Name), "Glue Job")
			continue
		}
		log.Printf("Successfully tagged Glue job: %s", aws.ToString(job.Name))
	}
}

// tagGlueCrawlers tags Glue crawlers
func (t *AWSResourceTagger) tagGlueCrawlers(client *glue.Client) {
	crawlers, err := client.GetCrawlers(t.ctx, &glue.GetCrawlersInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Crawlers")
		return
	}

	for _, crawler := range crawlers.Crawlers {
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: crawler.Name,
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(crawler.Name), "Glue Crawler")
			continue
		}
		log.Printf("Successfully tagged Glue crawler: %s", aws.ToString(crawler.Name))
	}
}

// tagGlueWorkflows tags Glue workflows
func (t *AWSResourceTagger) tagGlueWorkflows(client *glue.Client) {
	workflows, err := client.ListWorkflows(t.ctx, &glue.ListWorkflowsInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Workflows")
		return
	}

	for _, workflow := range workflows.Workflows {
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: aws.String(workflow),
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, workflow, "Glue Workflow")
			continue
		}
		log.Printf("Successfully tagged Glue workflow: %s", workflow)
	}
}

// tagGlueDevEndpoints tags Glue development endpoints
func (t *AWSResourceTagger) tagGlueDevEndpoints(client *glue.Client) {
	endpoints, err := client.GetDevEndpoints(t.ctx, &glue.GetDevEndpointsInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Dev Endpoints")
		return
	}

	for _, endpoint := range endpoints.DevEndpoints {
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: endpoint.EndpointName,
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(endpoint.EndpointName), "Glue Dev Endpoint")
			continue
		}
		log.Printf("Successfully tagged Glue dev endpoint: %s", aws.ToString(endpoint.EndpointName))
	}
}

// tagGlueMLTransforms tags Glue ML transforms
func (t *AWSResourceTagger) tagGlueMLTransforms(client *glue.Client) {
	transforms, err := client.GetMLTransforms(t.ctx, &glue.GetMLTransformsInput{})
	if err != nil {
		t.handleError(err, "all", "Glue ML Transforms")
		return
	}

	for _, transform := range transforms.Transforms {
		_, err := client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: transform.TransformId,
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(transform.Name), "Glue ML Transform")
			continue
		}
		log.Printf("Successfully tagged Glue ML transform: %s", aws.ToString(transform.Name))
	}
}

// convertToGlueTags converts the common tags map to Glue-specific tags
func (t *AWSResourceTagger) convertToGlueTags() map[string]string {
	return t.tags
}
