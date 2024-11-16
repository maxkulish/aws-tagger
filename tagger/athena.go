package tagger

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// AthenaAPI interface for Athena client operations
type AthenaAPI interface {
	ListWorkGroups(ctx context.Context, params *athena.ListWorkGroupsInput, optFns ...func(*athena.Options)) (*athena.ListWorkGroupsOutput, error)
	ListDataCatalogs(ctx context.Context, params *athena.ListDataCatalogsInput, optFns ...func(*athena.Options)) (*athena.ListDataCatalogsOutput, error)
	TagResource(ctx context.Context, params *athena.TagResourceInput, optFns ...func(*athena.Options)) (*athena.TagResourceOutput, error)
}

// validateTags checks if tags meet Athena's requirements
func (t *AWSResourceTagger) validateTags() error {
	if len(t.tags) > 50 {
		return fmt.Errorf("number of tags exceeds maximum limit of 50")
	}

	for key, value := range t.tags {
		if strings.HasPrefix(key, "aws:") {
			return fmt.Errorf("tag key cannot start with 'aws:': %s", key)
		}
		if len(key) < 1 || len(key) > 128 {
			return fmt.Errorf("tag key length must be between 1 and 128 characters: %s", key)
		}
		if len(value) > 256 {
			return fmt.Errorf("tag value length must not exceed 256 characters for key: %s", key)
		}
	}
	return nil
}

// buildAthenaWorkgroupARN builds the correct ARN format for Athena workgroups
func (t *AWSResourceTagger) buildAthenaWorkgroupARN(workgroupName string) string {
	// Add debug logging to verify the values
	arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s",
		t.cfg.Region,
		t.accountID,
		workgroupName)
	return arn
}

// buildAthenaCatalogARN builds the correct ARN format for Athena data catalogs
func (t *AWSResourceTagger) buildAthenaCatalogARN(catalogName string) string {
	// Add debug logging to verify the values
	arn := fmt.Sprintf("arn:aws:athena:%s:%s:datacatalog/%s",
		t.cfg.Region,
		t.accountID,
		catalogName)
	return arn
}

// tagAthenaWorkgroups tags Athena workgroups
func (t *AWSResourceTagger) tagAthenaWorkgroups(client AthenaAPI) error {
	input := &athena.ListWorkGroupsInput{}
	for {
		workgroups, err := client.ListWorkGroups(t.ctx, input)
		if err != nil {
			return fmt.Errorf("failed to list workgroups: %w", err)
		}

		for _, workgroup := range workgroups.WorkGroups {
			wgName := aws.ToString(workgroup.Name)
			if wgName == "primary" { // Skip the primary workgroup
				continue
			}

			arn := t.buildCompoundARN(AthenaWorkgroup, wgName)
			if err := t.tagResource(client, arn, wgName, "workgroup"); err != nil {
				// Log the error with more details
				log.Printf("Warning: failed to tag workgroup %s (ARN: %s): %v", wgName, arn, err)
				continue
			}
		}

		if workgroups.NextToken == nil {
			break
		}
		input.NextToken = workgroups.NextToken
	}
	return nil
}

// tagAthenaDataCatalogs tags Athena data catalogs
func (t *AWSResourceTagger) tagAthenaDataCatalogs(client AthenaAPI) error {
	log.Println("Starting to list and tag data catalogs...")
	input := &athena.ListDataCatalogsInput{}
	for {
		catalogs, err := client.ListDataCatalogs(t.ctx, input)
		if err != nil {
			return fmt.Errorf("failed to list data catalogs: %w", err)
		}

		for _, catalog := range catalogs.DataCatalogsSummary {
			catalogName := aws.ToString(catalog.CatalogName)
			// Removed the AwsDataCatalog skip condition

			arn := t.buildCompoundARN(AthenaCatalog, catalogName)
			if err := t.tagResource(client, arn, catalogName, "data catalog"); err != nil {
				// Log the error with more details
				log.Printf("Warning: failed to tag data catalog %s (ARN: %s): %v", catalogName, arn, err)
				continue
			}
		}

		if catalogs.NextToken == nil {
			break
		}
		input.NextToken = catalogs.NextToken
	}
	return nil
}

// tagResource handles the actual tagging operation with error handling
func (t *AWSResourceTagger) tagResource(client AthenaAPI, arn, resourceName, resourceType string) error {
	_, err := client.TagResource(t.ctx, &athena.TagResourceInput{
		ResourceARN: aws.String(arn),
		Tags:        t.convertToAthenaTags(),
	})
	if err != nil {
		return fmt.Errorf("failed to tag resource: %w", err)
	}
	log.Printf("Successfully tagged Athena %s: %s", resourceType, resourceName)
	return nil
}

// convertToAthenaTags converts the common tags map to Athena-specific tags
func (t *AWSResourceTagger) convertToAthenaTags() []athenatypes.Tag {
	athenaTags := make([]athenatypes.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		athenaTags = append(athenaTags, athenatypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return athenaTags
}

// tagAthenaResources is the main entry point that creates and uses the client
func (t *AWSResourceTagger) tagAthenaResources() {
	client := athena.NewFromConfig(t.cfg)
	t.tagAthenaResourcesWithClient(client)
}

// tagAthenaResourcesWithClient handles the actual tagging logic with a provided client
func (t *AWSResourceTagger) tagAthenaResourcesWithClient(client AthenaAPI) {
	log.Println("Tagging Athena resources...") // This must be the first log message
	log.Printf("Starting Athena tagging with Account ID: %s", t.accountID)

	// Validate tags before proceeding
	if err := t.validateTags(); err != nil {
		log.Printf("Error: Invalid tags configuration: %v", err)
		log.Println("Completed tagging Athena resources")
		return
	}

	// Tag workgroups
	if err := t.tagAthenaWorkgroups(client); err != nil {
		log.Printf("Error tagging Athena workgroups: %v", err)
	}

	// Tag data catalogs
	if err := t.tagAthenaDataCatalogs(client); err != nil {
		log.Printf("Error tagging Athena data catalogs: %v", err)
	}

	log.Println("Completed tagging Athena resources")
}
