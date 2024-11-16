package tagger

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"reflect"
	"sort"
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

// Helper function to match S3 PutBucketTaggingInput regardless of tag order
func matchS3TagsInput(expected *s3.PutBucketTaggingInput) func(*s3.PutBucketTaggingInput) bool {
	return func(actual *s3.PutBucketTaggingInput) bool {
		if !reflect.DeepEqual(expected.Bucket, actual.Bucket) {
			return false
		}

		expectedTags := expected.Tagging.TagSet
		actualTags := actual.Tagging.TagSet

		if len(expectedTags) != len(actualTags) {
			return false
		}

		// Sort both expected and actual tags for comparison
		sortedExpected := make([]s3types.Tag, len(expectedTags))
		sortedActual := make([]s3types.Tag, len(actualTags))
		copy(sortedExpected, expectedTags)
		copy(sortedActual, actualTags)

		sort.Slice(sortedExpected, func(i, j int) bool {
			return aws.ToString(sortedExpected[i].Key) < aws.ToString(sortedExpected[j].Key)
		})
		sort.Slice(sortedActual, func(i, j int) bool {
			return aws.ToString(sortedActual[i].Key) < aws.ToString(sortedActual[j].Key)
		})

		return reflect.DeepEqual(sortedExpected, sortedActual)
	}
}

func TestTagS3BucketsWithClient(t *testing.T) {
	tests := []struct {
		name          string
		tags          map[string]string
		buckets       []s3types.Bucket
		listError     error
		taggingErrors map[string]error
		expected      *S3Metrics
		setupMocks    bool // Add this field to control mock setup
	}{
		{
			name: "Successfully tag multiple buckets",
			tags: map[string]string{"env": "prod"},
			buckets: []s3types.Bucket{
				{Name: aws.String("bucket1")},
				{Name: aws.String("bucket2")},
			},
			expected: &S3Metrics{
				BucketsFound:  2,
				BucketsTagged: 2,
				BucketsFailed: 0,
			},
			setupMocks: true,
		},
		{
			name: "Empty tags map",
			tags: map[string]string{},
			expected: &S3Metrics{
				BucketsFound:  0,
				BucketsTagged: 0,
				BucketsFailed: 0,
			},
			setupMocks: false, // Don't set up mocks for empty tags
		},
		{
			name:      "Handle ListBuckets error",
			tags:      map[string]string{"env": "prod"},
			listError: errors.New("list error"),
			expected: &S3Metrics{
				BucketsFound:  0,
				BucketsTagged: 0,
				BucketsFailed: 0,
			},
			setupMocks: true,
		},
		{
			name: "Handle PutBucketTagging errors",
			tags: map[string]string{"env": "prod"},
			buckets: []s3types.Bucket{
				{Name: aws.String("bucket1")},
				{Name: aws.String("bucket2")},
			},
			taggingErrors: map[string]error{
				"bucket1": errors.New("tagging error"),
			},
			expected: &S3Metrics{
				BucketsFound:  2,
				BucketsTagged: 1,
				BucketsFailed: 1,
			},
			setupMocks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockS3Client)

			// Only set up mocks if setupMocks is true
			if tt.setupMocks {
				// Setup ListBuckets mock
				mockClient.On("ListBuckets", mock.Anything, mock.Anything).
					Return(&s3.ListBucketsOutput{Buckets: tt.buckets}, tt.listError)

				// Setup PutBucketTagging mocks
				for _, bucket := range tt.buckets {
					bucketName := aws.ToString(bucket.Name)
					mockClient.On("PutBucketTagging", mock.Anything, mock.MatchedBy(func(input *s3.PutBucketTaggingInput) bool {
						return aws.ToString(input.Bucket) == bucketName
					})).Return(&s3.PutBucketTaggingOutput{}, tt.taggingErrors[bucketName])
				}
			}

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			metrics := tagger.tagS3BucketsWithClient(mockClient)
			assert.Equal(t, tt.expected, metrics)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagBucket(t *testing.T) {
	tests := []struct {
		name        string
		bucketName  string
		tags        map[string]string
		expectError bool
	}{
		{
			name:        "Empty bucket name",
			bucketName:  "",
			tags:        map[string]string{"env": "prod"},
			expectError: true,
		},
		{
			name:        "Valid bucket name and tags",
			bucketName:  "test-bucket",
			tags:        map[string]string{"env": "prod"},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockS3Client)

			if tt.bucketName != "" {
				mockClient.On("PutBucketTagging", mock.Anything, mock.MatchedBy(func(input *s3.PutBucketTaggingInput) bool {
					return aws.ToString(input.Bucket) == tt.bucketName
				})).Return(&s3.PutBucketTaggingOutput{}, nil)
			}

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			err := tagger.tagBucket(mockClient, tt.bucketName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConvertToS3Tags(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected int
	}{
		{
			name:     "Nil tags map",
			input:    nil,
			expected: 0,
		},
		{
			name:     "Empty tags map",
			input:    map[string]string{},
			expected: 0,
		},
		{
			name: "Valid tags",
			input: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			expected: 2,
		},
		{
			name: "Tags with empty key",
			input: map[string]string{
				"":     "value",
				"team": "platform",
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToS3Tags(tt.input)
			assert.Equal(t, tt.expected, len(result))

			for _, tag := range result {
				assert.NotEmpty(t, aws.ToString(tag.Key))
				assert.NotNil(t, tag.Value)
			}
		})
	}
}

func TestTagS3BucketsWithClient_EmptyTags_Logging(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	mockClient := new(MockS3Client)
	tagger := &AWSResourceTagger{
		ctx:  context.Background(),
		tags: map[string]string{},
	}

	tagger.tagS3BucketsWithClient(mockClient)

	// Verify log message
	assert.Contains(t, buf.String(), "No tags provided, skipping S3 bucket tagging")

	// Verify no calls were made to the mock
	mockClient.AssertNotCalled(t, "ListBuckets")
	mockClient.AssertNotCalled(t, "PutBucketTagging")
}
