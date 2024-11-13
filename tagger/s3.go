package tagger

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
)

// tagS3Buckets tags S3 buckets
func (t *AWSResourceTagger) tagS3Buckets() {
	client := s3.NewFromConfig(t.cfg)

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
