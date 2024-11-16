package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockS3Client is a mock implementation of S3API
type MockS3Client struct {
	mock.Mock
}

func (m *MockS3Client) ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.ListBucketsOutput), args.Error(1)
}

func (m *MockS3Client) PutBucketTagging(ctx context.Context, params *s3.PutBucketTaggingInput, optFns ...func(*s3.Options)) (*s3.PutBucketTaggingOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*s3.PutBucketTaggingOutput), args.Error(1)
}

func TestTagS3Buckets(t *testing.T) {
	tests := []struct {
		name          string
		buckets       []s3types.Bucket
		tags          map[string]string
		listError     error
		taggingErrors map[string]error
		expectedCalls int
	}{
		{
			name: "Successfully tag multiple buckets",
			buckets: []s3types.Bucket{
				{Name: aws.String("bucket1")},
				{Name: aws.String("bucket2")},
			},
			tags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			taggingErrors: map[string]error{},
			expectedCalls: 2,
		},
		{
			name:          "Handle ListBuckets error",
			buckets:       []s3types.Bucket{},
			tags:          map[string]string{"env": "prod"},
			listError:     errors.New("ListBuckets failed"),
			expectedCalls: 0,
		},
		{
			name: "Handle PutBucketTagging error for some buckets",
			buckets: []s3types.Bucket{
				{Name: aws.String("bucket1")},
				{Name: aws.String("bucket2")},
			},
			tags: map[string]string{"env": "prod"},
			taggingErrors: map[string]error{
				"bucket1": errors.New("PutBucketTagging failed"),
			},
			expectedCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockS3Client)

			// Setup ListBuckets mock
			listBucketsOutput := &s3.ListBucketsOutput{Buckets: tt.buckets}
			mockClient.On("ListBuckets", mock.Anything, mock.Anything).Return(listBucketsOutput, tt.listError)

			// Setup PutBucketTagging mocks
			for _, bucket := range tt.buckets {
				expectedInput := &s3.PutBucketTaggingInput{
					Bucket: bucket.Name,
					Tagging: &s3types.Tagging{
						TagSet: convertToS3Tags(tt.tags),
					},
				}
				err := tt.taggingErrors[*bucket.Name]
				mockClient.On("PutBucketTagging", mock.Anything, expectedInput).Return(&s3.PutBucketTaggingOutput{}, err)
			}

			// Create tagger instance
			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			// Execute
			tagger.tagS3BucketsWithClient(mockClient)

			// Verify expectations
			mockClient.AssertNumberOfCalls(t, "PutBucketTagging", tt.expectedCalls)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConvertToS3Tags(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected []s3types.Tag
	}{
		{
			name: "Convert multiple tags",
			input: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			expected: []s3types.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("platform")},
			},
		},
		{
			name:     "Empty tags map",
			input:    map[string]string{},
			expected: []s3types.Tag{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToS3Tags(tt.input)
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}
