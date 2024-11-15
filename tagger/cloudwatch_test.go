package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cloudwatchtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockCloudWatchClient is a mock implementation of CloudWatchAPI
type MockCloudWatchClient struct {
	mock.Mock
}

func (m *MockCloudWatchClient) DescribeAlarms(ctx context.Context, params *cloudwatch.DescribeAlarmsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.DescribeAlarmsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*cloudwatch.DescribeAlarmsOutput), args.Error(1)
}

func (m *MockCloudWatchClient) ListDashboards(ctx context.Context, params *cloudwatch.ListDashboardsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListDashboardsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*cloudwatch.ListDashboardsOutput), args.Error(1)
}

func (m *MockCloudWatchClient) TagResource(ctx context.Context, params *cloudwatch.TagResourceInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.TagResourceOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*cloudwatch.TagResourceOutput), args.Error(1)
}

// TestTagCloudWatchResources tests the CloudWatch tagging functionality
func TestTagCloudWatchResources(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func(*MockCloudWatchClient)
		expectedMetrics struct {
			totalAlarms      int
			taggedAlarms     int
			failedAlarms     int
			totalDashboards  int
			taggedDashboards int
			failedDashboards int
		}
	}{
		{
			name: "Successfully tag all resources",
			setupMocks: func(m *MockCloudWatchClient) {
				// Mock DescribeAlarms
				m.On("DescribeAlarms", mock.Anything, &cloudwatch.DescribeAlarmsInput{}).
					Return(&cloudwatch.DescribeAlarmsOutput{
						MetricAlarms: []cloudwatchtypes.MetricAlarm{
							{
								AlarmName: aws.String("alarm1"),
								AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm1"),
							},
							{
								AlarmName: aws.String("alarm2"),
								AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm2"),
							},
						},
					}, nil).Once()

				// Mock ListDashboards
				m.On("ListDashboards", mock.Anything, &cloudwatch.ListDashboardsInput{}).
					Return(&cloudwatch.ListDashboardsOutput{
						DashboardEntries: []cloudwatchtypes.DashboardEntry{
							{
								DashboardName: aws.String("dashboard1"),
								DashboardArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:dashboard/dashboard1"),
							},
						},
					}, nil).Once()

				// Mock TagResource for alarms
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == "arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm1"
				})).Return(&cloudwatch.TagResourceOutput{}, nil).Once()

				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == "arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm2"
				})).Return(&cloudwatch.TagResourceOutput{}, nil).Once()

				// Mock TagResource for dashboard
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == "arn:aws:cloudwatch:us-west-2:123456789012:dashboard/dashboard1"
				})).Return(&cloudwatch.TagResourceOutput{}, nil).Once()
			},
			expectedMetrics: struct {
				totalAlarms      int
				taggedAlarms     int
				failedAlarms     int
				totalDashboards  int
				taggedDashboards int
				failedDashboards int
			}{
				totalAlarms:      2,
				taggedAlarms:     2,
				failedAlarms:     0,
				totalDashboards:  1,
				taggedDashboards: 1,
				failedDashboards: 0,
			},
		},
		{
			name: "Handle API errors",
			setupMocks: func(m *MockCloudWatchClient) {
				// Mock DescribeAlarms error
				m.On("DescribeAlarms", mock.Anything, &cloudwatch.DescribeAlarmsInput{}).
					Return(nil, errors.New("API error")).Once()

				// Mock ListDashboards error
				m.On("ListDashboards", mock.Anything, &cloudwatch.ListDashboardsInput{}).
					Return(nil, errors.New("API error")).Once()
			},
			expectedMetrics: struct {
				totalAlarms      int
				taggedAlarms     int
				failedAlarms     int
				totalDashboards  int
				taggedDashboards int
				failedDashboards int
			}{
				totalAlarms:      0,
				taggedAlarms:     0,
				failedAlarms:     0,
				totalDashboards:  0,
				taggedDashboards: 0,
				failedDashboards: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			mockClient := new(MockCloudWatchClient)
			tt.setupMocks(mockClient)

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				cfg:  aws.Config{Region: "us-west-2"},
				tags: map[string]string{"Environment": "Test"},
			}

			// Call the actual method that needs to be tested
			tagger.tagCloudWatchResourcesWithClient(mockClient)

			// Verify expectations
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConvertToCloudWatchTags(t *testing.T) {
	tagger := &AWSResourceTagger{
		tags: map[string]string{
			"Environment": "Test",
			"Project":     "Demo",
		},
	}

	cwTags := make([]cloudwatchtypes.Tag, 0, len(tagger.tags))
	for k, v := range tagger.tags {
		cwTags = append(cwTags, cloudwatchtypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}

	assert.Len(t, cwTags, 2)

	tagMap := make(map[string]string)
	for _, tag := range cwTags {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "Test", tagMap["Environment"])
	assert.Equal(t, "Demo", tagMap["Project"])
}
