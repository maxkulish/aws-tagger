package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
)

// tagGlueResources tags AWS Glue resources
func (t *AWSResourceTagger) tagGlueResources() {
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

// tagGlueDatabases tags Glue databases (skipping tables since they're not taggable)
func (t *AWSResourceTagger) tagGlueDatabases(client *glue.Client) {
	databases, err := client.GetDatabases(t.ctx, &glue.GetDatabasesInput{})
	if err != nil {
		t.handleError(err, "all", "Glue Databases")
		return
	}

	for _, db := range databases.DatabaseList {
		dbName := aws.ToString(db.Name)
		if err := t.tagDatabase(client, dbName); err != nil {
			log.Printf("Error processing database %s: %v", dbName, err)
			continue
		}
	}
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
