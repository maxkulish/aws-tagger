package tagger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// AWSResourceTagger handles AWS resource tagging operations
type AWSResourceTagger struct {
	ctx     context.Context
	cfg     aws.Config
	tags    map[string]string
	awsTags []types.Tag
}

// TagAllResources tags all supported resources
func (t *AWSResourceTagger) TagAllResources() {
	log.Println("Starting MAP 2.0 resource tagging process...")

	taggers := []func(){
		t.tagEC2Resources,
		t.tagS3Buckets,
		t.tagCloudWatchResources,
		t.tagOpenSearchResources,
		t.tagElastiCacheResources,
		t.tagRDSResources,
	}

	for _, tagger := range taggers {
		tagger()
		time.Sleep(time.Second) // Prevent API throttling
	}

	log.Println("Completed MAP 2.0 resource tagging process")
}

// NewAWSResourceTagger creates a new tagger instance
func NewAWSResourceTagger(ctx context.Context, profile, region string, tags map[string]string) (*AWSResourceTagger, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithSharedConfigProfile(profile),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	awsTags := make([]types.Tag, 0, len(tags))
	for k, v := range tags {
		awsTags = append(awsTags, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}

	return &AWSResourceTagger{
		ctx:     ctx,
		cfg:     cfg,
		tags:    tags,
		awsTags: awsTags,
	}, nil
}

// handleError handles AWS API errors
func (t *AWSResourceTagger) handleError(err error, resourceID, service string) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "AccessDenied":
			log.Printf("Access denied while tagging %s resource %s", service, resourceID)
		case "ResourceNotFoundException":
			log.Printf("Resource %s not found in %s", resourceID, service)
		default:
			log.Printf("Error tagging %s resource %s: %v", service, resourceID, err)
		}
	} else {
		log.Printf("Error tagging %s resource %s: %v", service, resourceID, err)
	}
}
