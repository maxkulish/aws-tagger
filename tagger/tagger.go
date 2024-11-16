package tagger

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// AWSResourceTagger handles AWS resource tagging operations
type AWSResourceTagger struct {
	ctx       context.Context
	cfg       aws.Config
	tags      map[string]string
	awsTags   []types.Tag
	accountID string
	region    string
}

const apiThrottleSleepDuration = time.Second

// TagAllResources concurrently tags all supported resources
func (t *AWSResourceTagger) TagAllResources() {
	log.Println("Starting MAP 2.0 resource tagging process...")

	if err := t.validateSSOSession(); err != nil {
		log.Fatalf("SSO session validation failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	resourceTaggers := map[string]func(){
		"EC2":        t.tagEC2Resources,
		"CloudWatch": t.tagCloudWatchResources,
		"Glue":       t.tagGlueResources,
		"Athena":     t.tagAthenaResources,
		//"S3Buckets":   t.tagS3Buckets,
		//"OpenSearch":  t.tagOpenSearchResources,
		//"ElastiCache": t.tagElastiCacheResources,
		//"RDS":         t.tagRDSResources,
		//"VPC":         t.tagVPCResources,
		//"ELB":         t.tagELBResources,
	}
	errorsChannel := make(chan error, len(resourceTaggers))

	for key, tagger := range resourceTaggers {
		wg.Add(1)
		go t.executeWithThrottleConcurrent(tagger, &wg, errorsChannel, key)
	}

	wg.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			log.Printf("Error in tagging process: %v", err)
		}
	}
	log.Println("Completed MAP 2.0 resource tagging process")
}

// executeWithThrottleConcurrent runs a function in a goroutine and then sleeps to prevent API throttling
func (t *AWSResourceTagger) executeWithThrottleConcurrent(f func(), wg *sync.WaitGroup, errorsChannel chan<- error, resourceType string) {
	defer wg.Done()
	log.Printf("Starting tagging for resource type: %s", resourceType)
	f()
	log.Printf("Completed tagging for resource type: %s", resourceType)
	time.Sleep(apiThrottleSleepDuration)
}

// validateSSOSession validates the SSO session by making a simple AWS API call
func (t *AWSResourceTagger) validateSSOSession() error {
	stsClient := sts.NewFromConfig(t.cfg)
	_, err := stsClient.GetCallerIdentity(t.ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("unable to validate SSO session: %v", err)
	}
	return nil
}

// getAccountID retrieves the AWS account ID using STS
func getAccountID(ctx context.Context, cfg aws.Config) (string, error) {
	stsClient := sts.NewFromConfig(cfg)
	result, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("unable to get caller identity: %w", err)
	}
	return *result.Account, nil
}

// NewAWSResourceTagger creates a new tagger instance
func NewAWSResourceTagger(ctx context.Context, profile, region string, tags map[string]string) (*AWSResourceTagger, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithSharedConfigProfile(profile),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	// Get AWS Account ID
	accountID, err := getAccountID(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to get AWS account ID: %v", err)
	}
	log.Printf("Using AWS Account ID: %s", accountID)

	// Convert tags to AWS format
	awsTags := make([]types.Tag, 0, len(tags))
	for k, v := range tags {
		awsTags = append(awsTags, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}

	return &AWSResourceTagger{
		ctx:       ctx,
		cfg:       cfg,
		tags:      tags,
		awsTags:   awsTags,
		accountID: accountID,
		region:    region,
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
