package tagger

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockRDSClient is a mock implementation of RDSAPI
type MockRDSClient struct {
	mock.Mock
}

func (m *MockRDSClient) DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rds.DescribeDBInstancesOutput), args.Error(1)
}

func (m *MockRDSClient) DescribeDBClusters(ctx context.Context, params *rds.DescribeDBClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rds.DescribeDBClustersOutput), args.Error(1)
}

func (m *MockRDSClient) DescribeDBSnapshots(ctx context.Context, params *rds.DescribeDBSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rds.DescribeDBSnapshotsOutput), args.Error(1)
}

func (m *MockRDSClient) DescribeDBClusterSnapshots(ctx context.Context, params *rds.DescribeDBClusterSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClusterSnapshotsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rds.DescribeDBClusterSnapshotsOutput), args.Error(1)
}

func (m *MockRDSClient) AddTagsToResource(ctx context.Context, params *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*rds.AddTagsToResourceOutput), args.Error(1)
}

// Helper function to compare AddTagsToResourceInput regardless of tag order
func matchTagsInput(expected *rds.AddTagsToResourceInput) func(*rds.AddTagsToResourceInput) bool {
	return func(actual *rds.AddTagsToResourceInput) bool {
		if !reflect.DeepEqual(expected.ResourceName, actual.ResourceName) {
			return false
		}

		if len(expected.Tags) != len(actual.Tags) {
			return false
		}

		// Sort both expected and actual tags for comparison
		sortedExpected := make([]rdstypes.Tag, len(expected.Tags))
		sortedActual := make([]rdstypes.Tag, len(actual.Tags))
		copy(sortedExpected, expected.Tags)
		copy(sortedActual, actual.Tags)

		sort.Slice(sortedExpected, func(i, j int) bool {
			return aws.ToString(sortedExpected[i].Key) < aws.ToString(sortedExpected[j].Key)
		})
		sort.Slice(sortedActual, func(i, j int) bool {
			return aws.ToString(sortedActual[i].Key) < aws.ToString(sortedActual[j].Key)
		})

		return reflect.DeepEqual(sortedExpected, sortedActual)
	}
}

func TestConvertToRDSTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     map[string]string
		expected []rdstypes.Tag
	}{
		{
			name: "Multiple tags",
			tags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			expected: []rdstypes.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("platform")},
			},
		},
		{
			name:     "Empty tags",
			tags:     map[string]string{},
			expected: []rdstypes.Tag{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tagger := &AWSResourceTagger{tags: tt.tags}
			result := tagger.convertToRDSTags()
			assert.ElementsMatch(t, tt.expected, result)
		})
	}
}

func TestTagDBInstances(t *testing.T) {
	tests := []struct {
		name          string
		instances     []rdstypes.DBInstance
		tags          map[string]string
		describeError error
		tagErrors     map[string]error
		expectedCalls int
	}{
		{
			name: "Successfully tag multiple instances",
			instances: []rdstypes.DBInstance{
				{
					DBInstanceIdentifier: aws.String("db-1"),
					DBInstanceArn:        aws.String("arn:aws:rds:region:account:db:db-1"),
				},
				{
					DBInstanceIdentifier: aws.String("db-2"),
					DBInstanceArn:        aws.String("arn:aws:rds:region:account:db:db-2"),
				},
			},
			tags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			expectedCalls: 2,
		},
		{
			name:          "Handle DescribeDBInstances error",
			describeError: errors.New("DescribeDBInstances failed"),
			expectedCalls: 0,
		},
		{
			name: "Handle AddTagsToResource error",
			instances: []rdstypes.DBInstance{
				{
					DBInstanceIdentifier: aws.String("db-1"),
					DBInstanceArn:        aws.String("arn:aws:rds:region:account:db:db-1"),
				},
			},
			tagErrors: map[string]error{
				"arn:aws:rds:region:account:db:db-1": errors.New("AddTagsToResource failed"),
			},
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockRDSClient)

			// Setup DescribeDBInstances mock
			describeOutput := &rds.DescribeDBInstancesOutput{DBInstances: tt.instances}
			mockClient.On("DescribeDBInstances", mock.Anything, mock.Anything).Return(describeOutput, tt.describeError)

			// Setup AddTagsToResource mocks
			for _, instance := range tt.instances {
				expectedInput := &rds.AddTagsToResourceInput{
					ResourceName: instance.DBInstanceArn,
					Tags:         convertToRDSTags(tt.tags),
				}
				err := tt.tagErrors[*instance.DBInstanceArn]
				mockClient.On("AddTagsToResource", mock.Anything, expectedInput).Return(&rds.AddTagsToResourceOutput{}, err)
			}

			// Create tagger instance
			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			// Execute
			tagger.tagDBInstancesWithClient(mockClient)

			// Verify expectations
			mockClient.AssertNumberOfCalls(t, "AddTagsToResource", tt.expectedCalls)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagDBClusters(t *testing.T) {
	tests := []struct {
		name          string
		clusters      []rdstypes.DBCluster
		tags          map[string]string
		describeError error
		tagErrors     map[string]error
		expectedCalls int
	}{
		{
			name: "Successfully tag multiple clusters",
			clusters: []rdstypes.DBCluster{
				{
					DBClusterIdentifier: aws.String("cluster-1"),
					DBClusterArn:        aws.String("arn:aws:rds:region:account:cluster:cluster-1"),
				},
				{
					DBClusterIdentifier: aws.String("cluster-2"),
					DBClusterArn:        aws.String("arn:aws:rds:region:account:cluster:cluster-2"),
				},
			},
			tags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			expectedCalls: 2,
		},
		{
			name:          "Handle DescribeDBClusters error",
			describeError: errors.New("DescribeDBClusters failed"),
			expectedCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockRDSClient)

			// Setup DescribeDBClusters mock
			describeOutput := &rds.DescribeDBClustersOutput{DBClusters: tt.clusters}
			mockClient.On("DescribeDBClusters", mock.Anything, mock.Anything).Return(describeOutput, tt.describeError)

			// Setup AddTagsToResource mocks
			for _, cluster := range tt.clusters {
				expectedInput := &rds.AddTagsToResourceInput{
					ResourceName: cluster.DBClusterArn,
					Tags:         convertToRDSTags(tt.tags),
				}

				mockClient.On("AddTagsToResource",
					mock.Anything,
					mock.MatchedBy(matchTagsInput(expectedInput)),
				).Return(&rds.AddTagsToResourceOutput{}, tt.tagErrors[*cluster.DBClusterArn])
			}

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			tagger.tagDBClustersWithClient(mockClient)

			mockClient.AssertNumberOfCalls(t, "AddTagsToResource", tt.expectedCalls)
			mockClient.AssertExpectations(t)
		})
	}
}

// convertToRDSTags converts a map of tags to a slice of rdstypes.Tag to be used with AWS RDS operations.
func convertToRDSTags(tags map[string]string) []rdstypes.Tag {
	rdsTags := make([]rdstypes.Tag, 0, len(tags))
	for k, v := range tags {
		rdsTags = append(rdsTags, rdstypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return rdsTags
}
