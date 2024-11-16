package tagger

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

// GlueMetrics tracks the number of resources tagged
type GlueMetrics struct {
	DatabasesFound    int32
	DatabasesTagged   int32
	DatabasesFailed   int32
	ConnectionsFound  int32
	ConnectionsTagged int32
	ConnectionsFailed int32
}

// tagGlueResources tags AWS Glue resources
func (t *AWSResourceTagger) tagGlueResources() {
	log.Println("Tagging Glue resources...")
	client := glue.NewFromConfig(t.cfg)

	metrics := &GlueMetrics{}

	// Tag all supported Glue resource types
	t.tagGlueDatabases(client, metrics)
	t.tagGlueConnections(client, metrics)
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

	// Log final metrics
	log.Printf("Glue Tagging Summary:")
	log.Printf("Databases: Found: %d, Tagged: %d, Failed: %d",
		metrics.DatabasesFound, metrics.DatabasesTagged, metrics.DatabasesFailed)
	log.Printf("Connections: Found: %d, Tagged: %d, Failed: %d",
		metrics.ConnectionsFound, metrics.ConnectionsTagged, metrics.ConnectionsFailed)

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
