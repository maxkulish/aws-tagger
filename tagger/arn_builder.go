// arn.go
package tagger

import (
	"fmt"
	"strings"
)

// ResourceType represents the type of AWS resource
type ResourceType struct {
	Service    string
	Type       string
	ArnPattern string
}

var (
	AthenaWorkgroup = ResourceType{
		Service:    "athena",
		Type:       "workgroup",
		ArnPattern: "arn:aws:athena:%s:%s:workgroup/%s",
	}
	AthenaCatalog = ResourceType{
		Service:    "athena",
		Type:       "datacatalog",
		ArnPattern: "arn:aws:athena:%s:%s:datacatalog/%s",
	}
	GlueDatabase = ResourceType{
		Service:    "glue",
		Type:       "database",
		ArnPattern: "arn:aws:glue:%s:%s:database/%s",
	}
	GlueTable = ResourceType{
		Service:    "glue",
		Type:       "table",
		ArnPattern: "arn:aws:glue:%s:%s:table/%s",
	}
	GlueConnection = ResourceType{
		Service:    "glue",
		Type:       "connection",
		ArnPattern: "arn:aws:glue:%s:%s:connection/%s",
	}
	GlueCrawler = ResourceType{
		Service:    "glue",
		Type:       "crawler",
		ArnPattern: "arn:aws:glue:%s:%s:crawler/%s",
	}
	GlueJob = ResourceType{
		Service:    "glue",
		Type:       "job",
		ArnPattern: "arn:aws:glue:%s:%s:job/%s",
	}
	GlueTrigger = ResourceType{
		Service:    "glue",
		Type:       "trigger",
		ArnPattern: "arn:aws:glue:%s:%s:trigger/%s",
	}
	GlueWorkflow = ResourceType{
		Service:    "glue",
		Type:       "workflow",
		ArnPattern: "arn:aws:glue:%s:%s:workflow/%s",
	}
)

// cleanResourceName removes leading/trailing slashes and collapses multiple slashes into one
func cleanResourceName(name string) string {
	// Trim leading/trailing slashes
	name = strings.Trim(name, "/")
	// Replace multiple slashes with a single slash
	for strings.Contains(name, "//") {
		name = strings.ReplaceAll(name, "//", "/")
	}
	return name
}

// buildARN constructs the ARN for an AWS resource
func (t *AWSResourceTagger) buildARN(resourceType ResourceType, resourceName string) string {
	return fmt.Sprintf(
		resourceType.ArnPattern,
		t.region,
		t.accountID,
		cleanResourceName(resourceName),
	)
}

// buildCompoundARN constructs an ARN for resources that need multiple identifiers
func (t *AWSResourceTagger) buildCompoundARN(resourceType ResourceType, parts ...string) string {
	// Clean each part individually
	cleanParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if cleaned := cleanResourceName(part); cleaned != "" {
			cleanParts = append(cleanParts, cleaned)
		}
	}

	// Join the cleaned parts with a single slash
	resourceName := strings.Join(cleanParts, "/")

	return t.buildARN(resourceType, resourceName)
}
