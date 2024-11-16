package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// tagGlueResources tags AWS Glue resources
func (t *AWSResourceTagger) tagGlueResources() {
	fmt.Println("=====================================")
	log.Println("Tagging Glue resources...")
	client := glue.NewFromConfig(t.cfg)

	// Tag all supported Glue resource types
	t.tagGlueDatabases(client)
	//t.tagGlueConnections(client)
	//t.tagGlueCrawlers(client)
	//t.tagGlueJobs(client)
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

// tagGlueDatabases tags Glue databases and their tables
func (t *AWSResourceTagger) tagGlueDatabases(client *glue.Client) {
	databases, err := client.GetDatabases(t.ctx, &glue.GetDatabasesInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Databases")
		return
	}

	for _, db := range databases.DatabaseList {
		dbName := aws.ToString(db.Name)

		// Construct database ARN
		resourceArn := t.buildCompoundARN(GlueDatabase, dbName)
		log.Printf("database ARN: %s", resourceArn)

		_, err = client.TagResource(t.ctx, &glue.TagResourceInput{
			ResourceArn: aws.String(resourceArn),
			TagsToAdd:   t.convertToGlueTags(),
		})
		if err != nil {
			t.handleError(err, dbName, "Glue Database")
			continue
		}
		log.Printf("Successfully tagged Glue database: %s", dbName)

		// Tag tables in the database
		tables, err := client.GetTables(t.ctx, &glue.GetTablesInput{
			DatabaseName: aws.String(dbName),
		})
		if err != nil {
			t.handleError(err, dbName, "Glue Tables")
			continue
		}

		for _, table := range tables.TableList {
			tableName := aws.ToString(table.Name)

			// Construct table ARN
			// For tables, the resource name includes both database and table names
			tableArn := t.buildCompoundARN(GlueTable, fmt.Sprintf("%s/%s", dbName, tableName))
			log.Printf("=> tableArn: %s", tableArn)

			_, err = client.TagResource(t.ctx, &glue.TagResourceInput{
				ResourceArn: aws.String(tableArn),
				TagsToAdd:   t.convertToGlueTags(),
			})
			if err != nil {
				t.handleError(err, tableName, "Glue Table")
				continue
			}
			log.Printf("Successfully tagged Glue table: %s.%s", dbName, tableName)
		}
	}
}

// Helper function to get Glue resource ARN
func (t *AWSResourceTagger) getGlueResourceARN(client *glue.Client, resourceType string, names ...string) (string, error) {
	switch resourceType {
	case "database":
		if len(names) != 1 {
			return "", fmt.Errorf("database requires exactly one name")
		}
		resp, err := client.GetDatabase(t.ctx, &glue.GetDatabaseInput{
			Name: aws.String(names[0]),
		})
		if err != nil {
			return "", err
		}
		return aws.ToString(resp.Database.CatalogId), nil

	case "table":
		if len(names) != 2 {
			return "", fmt.Errorf("table requires database name and table name")
		}
		resp, err := client.GetTable(t.ctx, &glue.GetTableInput{
			DatabaseName: aws.String(names[0]),
			Name:         aws.String(names[1]),
		})
		if err != nil {
			return "", err
		}
		return aws.ToString(resp.Table.CatalogId), nil
	}
	return "", fmt.Errorf("unknown resource type: %s", resourceType)
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
