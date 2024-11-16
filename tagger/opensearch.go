package tagger

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	ostypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
)

// OpenSearchAPI interface for OpenSearch client operations
type OpenSearchAPI interface {
	ListDomainNames(ctx context.Context, params *opensearch.ListDomainNamesInput, optFns ...func(*opensearch.Options)) (*opensearch.ListDomainNamesOutput, error)
	DescribeDomain(ctx context.Context, params *opensearch.DescribeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DescribeDomainOutput, error)
	AddTags(ctx context.Context, params *opensearch.AddTagsInput, optFns ...func(*opensearch.Options)) (*opensearch.AddTagsOutput, error)
	ListTags(ctx context.Context, params *opensearch.ListTagsInput, optFns ...func(*opensearch.Options)) (*opensearch.ListTagsOutput, error)
}

// formatTags converts a slice of OpenSearch tags to a human-readable string
func formatTags(tags []ostypes.Tag) string {
	var tagPairs []string
	for _, tag := range tags {
		if tag.Key != nil && tag.Value != nil {
			tagPairs = append(tagPairs, fmt.Sprintf("%s: %s", *tag.Key, *tag.Value))
		}
	}
	return fmt.Sprintf("{%s}", strings.Join(tagPairs, ", "))
}

// tagOpenSearchResources is the main entry point that creates and uses the client
func (t *AWSResourceTagger) tagOpenSearchResources() {
	fmt.Println("====================================")
	log.Println("Starting OpenSearch resource tagging...")

	client := opensearch.NewFromConfig(t.cfg)
	t.tagOpenSearchResourcesWithClient(client)

	log.Println("Completed OpenSearch resource tagging")
}

// tagOpenSearchResourcesWithClient handles the actual tagging logic with a provided client
func (t *AWSResourceTagger) tagOpenSearchResourcesWithClient(client OpenSearchAPI) {
	// List all OpenSearch domains
	listDomainsOutput, err := client.ListDomainNames(t.ctx, &opensearch.ListDomainNamesInput{})
	if err != nil {
		t.handleError(err, "all", "OpenSearch")
		return
	}

	// Convert the generic tags map to OpenSearch TagList
	openSearchTags := convertToOpenSearchTags(t.tags)

	// Tag each domain
	for _, domain := range listDomainsOutput.DomainNames {
		domainName := aws.ToString(domain.DomainName)

		// Get the domain's ARN
		describeOutput, err := client.DescribeDomain(t.ctx, &opensearch.DescribeDomainInput{
			DomainName: domain.DomainName,
		})
		if err != nil {
			t.handleError(err, domainName, "OpenSearch")
			continue
		}

		// Add tags to the domain
		_, err = client.AddTags(t.ctx, &opensearch.AddTagsInput{
			ARN:     describeOutput.DomainStatus.ARN,
			TagList: openSearchTags,
		})
		if err != nil {
			t.handleError(err, domainName, "OpenSearch")
			log.Printf("Failed to tag OpenSearch domain: %s", domainName)
		} else {
			log.Printf("Successfully tagged OpenSearch domain: %s with tags %s",
				domainName, formatTags(openSearchTags))
		}

		// List current tags for verification
		listTagsOutput, err := client.ListTags(t.ctx, &opensearch.ListTagsInput{
			ARN: describeOutput.DomainStatus.ARN,
		})
		if err != nil {
			log.Printf("Error listing tags for OpenSearch domain %s: %v", domainName, err)
		} else {
			log.Printf("Current tags for OpenSearch domain %s: %s",
				domainName, formatTags(listTagsOutput.TagList))
		}
	}
}

// Helper function to convert tags map to OpenSearch tags
func convertToOpenSearchTags(tags map[string]string) []ostypes.Tag {
	openSearchTags := make([]ostypes.Tag, 0, len(tags))
	for k, v := range tags {
		openSearchTags = append(openSearchTags, ostypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return openSearchTags
}
