package tagger

import (
	"context"
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

// tagS3Buckets is the main entry point that creates and uses the client
func (t *AWSResourceTagger) tagS3Buckets() {
	client := s3.NewFromConfig(t.cfg)
	t.tagS3BucketsWithClient(client)
}

// tagS3BucketsWithClient handles the actual tagging logic with a provided client
func (t *AWSResourceTagger) tagS3BucketsWithClient(client S3API) {
	result, err := client.ListBuckets(t.ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Printf("Error listing S3 buckets: %v", err)
		return
	}

	for _, bucket := range result.Buckets {
		_, err := client.PutBucketTagging(t.ctx, &s3.PutBucketTaggingInput{
			Bucket: bucket.Name,
			Tagging: &s3types.Tagging{
				TagSet: convertToS3Tags(t.tags),
			},
		})
		if err != nil {
			t.handleError(err, *bucket.Name, "S3")
			continue
		}
		log.Printf("Tagged S3 bucket: %s", *bucket.Name)
	}
}

// convertToS3Tags converts generic tags to S3-specific tag format
func convertToS3Tags(tags map[string]string) []s3types.Tag {
	s3Tags := make([]s3types.Tag, 0, len(tags))
	for k, v := range tags {
		s3Tags = append(s3Tags, s3types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return s3Tags
}
