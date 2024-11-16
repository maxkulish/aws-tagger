// s3.go
package tagger

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3API interface for S3 client operations
type S3API interface {
	ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	PutBucketTagging(ctx context.Context, params *s3.PutBucketTaggingInput, optFns ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error)
}

// S3Metrics tracks the success/failure metrics for S3 tagging operations
type S3Metrics struct {
	BucketsFound  int
	BucketsTagged int
	BucketsFailed int
}

// tagS3Buckets is the main entry point that creates and uses the client
func (t *AWSResourceTagger) tagS3Buckets() {
	client := s3.NewFromConfig(t.cfg)
	metrics := t.tagS3BucketsWithClient(client)

	log.Printf("S3 Tagging Summary - Found: %d, Tagged: %d, Failed: %d",
		metrics.BucketsFound, metrics.BucketsTagged, metrics.BucketsFailed)
}

// tagS3BucketsWithClient handles the actual tagging logic with a provided client
func (t *AWSResourceTagger) tagS3BucketsWithClient(client S3API) *S3Metrics {
	metrics := &S3Metrics{}

	if len(t.tags) == 0 {
		log.Println("No tags provided, skipping S3 bucket tagging")
		return metrics
	}

	result, err := client.ListBuckets(t.ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.handleError(err, "all", "S3")
		return metrics
	}

	metrics.BucketsFound = len(result.Buckets)
	log.Printf("Found %d S3 buckets to tag", metrics.BucketsFound)

	for _, bucket := range result.Buckets {
		bucketName := aws.ToString(bucket.Name)
		if err := t.tagBucket(client, bucketName); err != nil {
			metrics.BucketsFailed++
			t.handleError(err, bucketName, "S3")
			continue
		}
		metrics.BucketsTagged++
		log.Printf("Successfully tagged S3 bucket: %s", bucketName)
	}

	return metrics
}

// tagBucket tags a single S3 bucket with the configured tags
func (t *AWSResourceTagger) tagBucket(client S3API, bucketName string) error {
	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	_, err := client.PutBucketTagging(t.ctx, &s3.PutBucketTaggingInput{
		Bucket: aws.String(bucketName),
		Tagging: &s3types.Tagging{
			TagSet: convertToS3Tags(t.tags),
		},
	})

	return err
}

// convertToS3Tags converts generic tags to S3-specific tag format
func convertToS3Tags(tags map[string]string) []s3types.Tag {
	if tags == nil {
		return []s3types.Tag{}
	}

	s3Tags := make([]s3types.Tag, 0, len(tags))
	for k, v := range tags {
		if k != "" { // Skip empty keys
			s3Tags = append(s3Tags, s3types.Tag{
				Key:   aws.String(k),
				Value: aws.String(v),
			})
		}
	}
	return s3Tags
}
