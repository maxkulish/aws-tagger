package tagger

import (
	"bytes"
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elctypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/aws/smithy-go"
	"log"
	"strings"
	"testing"
)

// MockElastiCacheClient is a mock implementation of ElastiCacheAPI
type MockElastiCacheClient struct {
	DescribeCacheClustersFunc     func(ctx context.Context, params *elasticache.DescribeCacheClustersInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error)
	DescribeReplicationGroupsFunc func(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error)
	AddTagsToResourceFunc         func(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error)
}

func (m *MockElastiCacheClient) DescribeCacheClusters(ctx context.Context, params *elasticache.DescribeCacheClustersInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error) {
	return m.DescribeCacheClustersFunc(ctx, params, optFns...)
}

func (m *MockElastiCacheClient) DescribeReplicationGroups(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
	return m.DescribeReplicationGroupsFunc(ctx, params, optFns...)
}

func (m *MockElastiCacheClient) AddTagsToResource(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error) {
	return m.AddTagsToResourceFunc(ctx, params, optFns...)
}

// mockAPIError implements the smithy.APIError interface
type mockAPIError struct {
	code    string
	message string
}

func (e *mockAPIError) Error() string {
	return e.message
}

func (e *mockAPIError) ErrorCode() string {
	return e.code
}

func (e *mockAPIError) ErrorMessage() string {
	return e.message
}

func (e *mockAPIError) ErrorFault() smithy.ErrorFault {
	return smithy.FaultUnknown
}

func TestTagElastiCacheResourcesWithClient(t *testing.T) {
	tests := []struct {
		name          string
		tags          map[string]string
		mockResponses *MockElastiCacheClient
		expectedLogs  []string
	}{
		{
			name: "successful tagging of clusters and replication groups",
			tags: map[string]string{"env": "prod", "team": "platform"},
			mockResponses: &MockElastiCacheClient{
				DescribeCacheClustersFunc: func(ctx context.Context, params *elasticache.DescribeCacheClustersInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error) {
					return &elasticache.DescribeCacheClustersOutput{
						CacheClusters: []elctypes.CacheCluster{
							{
								ARN:            aws.String("arn:aws:elasticache:region:account:cluster/test-cluster"),
								CacheClusterId: aws.String("test-cluster"),
							},
						},
					}, nil
				},
				DescribeReplicationGroupsFunc: func(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
					return &elasticache.DescribeReplicationGroupsOutput{
						ReplicationGroups: []elctypes.ReplicationGroup{
							{
								ARN:                aws.String("arn:aws:elasticache:region:account:replicationgroup/test-group"),
								ReplicationGroupId: aws.String("test-group"),
							},
						},
					}, nil
				},
				AddTagsToResourceFunc: func(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error) {
					return &elasticache.AddTagsToResourceOutput{}, nil
				},
			},
			expectedLogs: []string{
				"Successfully tagged ElastiCache cluster: test-cluster",
				"Successfully tagged ElastiCache replication group: test-group",
			},
		},
		{
			name: "access denied error",
			tags: map[string]string{"env": "prod"},
			mockResponses: &MockElastiCacheClient{
				DescribeCacheClustersFunc: func(ctx context.Context, params *elasticache.DescribeCacheClustersInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error) {
					return nil, &mockAPIError{
						code:    "AccessDenied",
						message: "Access denied",
					}
				},
			},
			expectedLogs: []string{
				"Access denied while tagging ElastiCache resource all",
			},
		},
		{
			name: "resource not found error",
			tags: map[string]string{"env": "prod"},
			mockResponses: &MockElastiCacheClient{
				DescribeCacheClustersFunc: func(ctx context.Context, params *elasticache.DescribeCacheClustersInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeCacheClustersOutput, error) {
					return &elasticache.DescribeCacheClustersOutput{
						CacheClusters: []elctypes.CacheCluster{
							{
								ARN:            aws.String("arn:aws:elasticache:region:account:cluster/test-cluster"),
								CacheClusterId: aws.String("test-cluster"),
							},
						},
					}, nil
				},
				AddTagsToResourceFunc: func(ctx context.Context, params *elasticache.AddTagsToResourceInput, optFns ...func(*elasticache.Options)) (*elasticache.AddTagsToResourceOutput, error) {
					return nil, &mockAPIError{
						code:    "ResourceNotFoundException",
						message: "Resource not found",
					}
				},
				DescribeReplicationGroupsFunc: func(ctx context.Context, params *elasticache.DescribeReplicationGroupsInput, optFns ...func(*elasticache.Options)) (*elasticache.DescribeReplicationGroupsOutput, error) {
					return &elasticache.DescribeReplicationGroupsOutput{}, nil
				},
			},
			expectedLogs: []string{
				"Resource arn:aws:elasticache:region:account:cluster/test-cluster not found in ElastiCache",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture log output
			var buf bytes.Buffer
			log.SetOutput(&buf)
			defer log.SetOutput(log.Default().Writer())

			// Create tagger
			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			// Execute the function
			tagger.tagElastiCacheResourcesWithClient(tt.mockResponses)

			// Check logs
			logs := buf.String()
			for _, expectedLog := range tt.expectedLogs {
				if !strings.Contains(logs, expectedLog) {
					t.Errorf("Expected log message '%s' not found in logs: %s", expectedLog, logs)
				}
			}
		})
	}
}
