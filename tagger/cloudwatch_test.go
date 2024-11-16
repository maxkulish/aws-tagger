package tagger

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/smithy-go"
	"log"
	"os"
	"strings"
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

func TestTagCloudWatchResourcesWithEmptyTags(t *testing.T) {
	ctx := context.Background()
	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		region:    "us-west-2",
		tags:      map[string]string{},
	}

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	defer log.SetOutput(os.Stderr)

	mockClient := new(MockCloudWatchClient)
	tagger.tagCloudWatchResourcesWithClient(mockClient)

	// Verify no API calls were made
	mockClient.AssertNotCalled(t, "DescribeAlarms")
	mockClient.AssertNotCalled(t, "ListDashboards")
	mockClient.AssertNotCalled(t, "TagResource")

	// Verify logs
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Starting CloudWatch resource tagging...")
	assert.Contains(t, logOutput, "No tags provided, skipping CloudWatch resource tagging")
	assert.Contains(t, logOutput, "Completed CloudWatch resource tagging")
}

func TestTagCloudWatchResources(t *testing.T) {
	ctx := context.Background()
	validTags := map[string]string{
		"Environment": "Test",
		"Project":     "Demo",
	}

	tests := []struct {
		name          string
		tags          map[string]string
		setupMocks    func(*MockCloudWatchClient)
		expectedCalls map[string]int
		expectError   bool
		checkLogs     func(string) bool
	}{
		{
			name: "Successfully tag multiple resources with pagination",
			tags: validTags,
			setupMocks: func(m *MockCloudWatchClient) {
				// First page of alarms
				m.On("DescribeAlarms", mock.Anything, &cloudwatch.DescribeAlarmsInput{
					NextToken: (*string)(nil),
				}).Return(&cloudwatch.DescribeAlarmsOutput{
					MetricAlarms: []cloudwatchtypes.MetricAlarm{
						{
							AlarmName: aws.String("alarm1"),
							AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm1"),
						},
					},
					NextToken: aws.String("next-token"),
				}, nil).Once()

				// Second page of alarms
				m.On("DescribeAlarms", mock.Anything, &cloudwatch.DescribeAlarmsInput{
					NextToken: aws.String("next-token"),
				}).Return(&cloudwatch.DescribeAlarmsOutput{
					MetricAlarms: []cloudwatchtypes.MetricAlarm{
						{
							AlarmName: aws.String("alarm2"),
							AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm2"),
						},
					},
				}, nil).Once()

				// First page of dashboards
				m.On("ListDashboards", mock.Anything, &cloudwatch.ListDashboardsInput{
					NextToken: (*string)(nil),
				}).Return(&cloudwatch.ListDashboardsOutput{
					DashboardEntries: []cloudwatchtypes.DashboardEntry{
						{
							DashboardName: aws.String("dashboard1"),
							DashboardArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:dashboard/dashboard1"),
						},
					},
					NextToken: aws.String("next-dash-token"),
				}, nil).Once()

				// Second page of dashboards
				m.On("ListDashboards", mock.Anything, &cloudwatch.ListDashboardsInput{
					NextToken: aws.String("next-dash-token"),
				}).Return(&cloudwatch.ListDashboardsOutput{
					DashboardEntries: []cloudwatchtypes.DashboardEntry{
						{
							DashboardName: aws.String("dashboard2"),
							DashboardArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:dashboard/dashboard2"),
						},
					},
				}, nil).Once()

				// Mock TagResource for all resources
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), ":alarm:") ||
						strings.Contains(aws.ToString(input.ResourceARN), ":dashboard/")
				})).Return(&cloudwatch.TagResourceOutput{}, nil)
			},
			expectedCalls: map[string]int{
				"DescribeAlarms": 2,
				"ListDashboards": 2,
				"TagResource":    4,
			},
		},
		{
			name: "Handle TagResource error for alarms",
			tags: validTags,
			setupMocks: func(m *MockCloudWatchClient) {
				m.On("DescribeAlarms", mock.Anything, mock.Anything).
					Return(&cloudwatch.DescribeAlarmsOutput{
						MetricAlarms: []cloudwatchtypes.MetricAlarm{
							{
								AlarmName: aws.String("alarm1"),
								AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:alarm1"),
							},
						},
					}, nil)

				m.On("ListDashboards", mock.Anything, mock.Anything).
					Return(&cloudwatch.ListDashboardsOutput{}, nil)

				// Mock TagResource to fail for alarms
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), ":alarm:")
				})).Return(nil, fmt.Errorf("API error")).Once()
			},
			expectedCalls: map[string]int{
				"DescribeAlarms": 1,
				"ListDashboards": 1,
				"TagResource":    1,
			},
		},
		{
			name: "Handle empty response from APIs",
			tags: validTags,
			setupMocks: func(m *MockCloudWatchClient) {
				m.On("DescribeAlarms", mock.Anything, mock.Anything).
					Return(&cloudwatch.DescribeAlarmsOutput{
						MetricAlarms: []cloudwatchtypes.MetricAlarm{},
					}, nil)

				m.On("ListDashboards", mock.Anything, mock.Anything).
					Return(&cloudwatch.ListDashboardsOutput{
						DashboardEntries: []cloudwatchtypes.DashboardEntry{},
					}, nil)
			},
			expectedCalls: map[string]int{
				"DescribeAlarms": 1,
				"ListDashboards": 1,
				"TagResource":    0,
			},
		},
		{
			name: "Handle AccessDenied error",
			tags: validTags,
			setupMocks: func(m *MockCloudWatchClient) {
				// Create an AWS API error
				apiErr := &smithy.GenericAPIError{
					Code:    "AccessDenied",
					Message: "User not authorized",
				}

				m.On("DescribeAlarms", mock.Anything, mock.Anything).
					Return(nil, apiErr)

				m.On("ListDashboards", mock.Anything, mock.Anything).
					Return(nil, apiErr)
			},
			expectedCalls: map[string]int{
				"DescribeAlarms": 1,
				"ListDashboards": 1,
				"TagResource":    0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockCloudWatchClient)
			tt.setupMocks(mockClient)

			tagger := &AWSResourceTagger{
				ctx:       ctx,
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tt.tags,
			}

			// Capture log output if needed for verification
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			defer log.SetOutput(os.Stderr)

			tagger.tagCloudWatchResourcesWithClient(mockClient)

			// Verify the number of calls for each method
			for method, expectedCount := range tt.expectedCalls {
				actualCount := 0
				for _, call := range mockClient.Calls {
					if call.Method == method {
						actualCount++
					}
				}
				assert.Equal(t, expectedCount, actualCount, "Expected %d calls to %s but got %d", expectedCount, method, actualCount)
			}

			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagCloudWatchResourcesMainFunction(t *testing.T) {
	tests := []struct {
		name       string
		tags       map[string]string
		setupMocks func(*MockCloudWatchClient)
		expectLogs []string
	}{
		{
			name: "Success case",
			tags: map[string]string{"Environment": "Test"},
			setupMocks: func(m *MockCloudWatchClient) {
				// Mock DescribeAlarms
				m.On("DescribeAlarms", mock.Anything, mock.Anything).
					Return(&cloudwatch.DescribeAlarmsOutput{
						MetricAlarms: []cloudwatchtypes.MetricAlarm{
							{
								AlarmName: aws.String("test-alarm"),
								AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:test-alarm"),
							},
						},
					}, nil)

				// Mock ListDashboards
				m.On("ListDashboards", mock.Anything, mock.Anything).
					Return(&cloudwatch.ListDashboardsOutput{
						DashboardEntries: []cloudwatchtypes.DashboardEntry{
							{
								DashboardName: aws.String("test-dashboard"),
								DashboardArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:dashboard/test-dashboard"),
							},
						},
					}, nil)

				// Mock TagResource
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *cloudwatch.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), ":alarm:") ||
						strings.Contains(aws.ToString(input.ResourceARN), ":dashboard/")
				})).Return(&cloudwatch.TagResourceOutput{}, nil)
			},
			expectLogs: []string{
				"Starting CloudWatch resource tagging...",
				"Discovering CloudWatch alarms...",
				"Discovering CloudWatch dashboards...",
				"CloudWatch Tagging Summary:",
				"Alarms: Total=1, Tagged=1, Failed=0",
				"Dashboards: Total=1, Tagged=1, Failed=0",
				"Completed CloudWatch resource tagging",
			},
		},
		{
			name: "Empty tags case",
			tags: map[string]string{},
			setupMocks: func(m *MockCloudWatchClient) {
				// No mocks needed as no calls should be made
			},
			expectLogs: []string{
				"Starting CloudWatch resource tagging...",
				"No tags provided, skipping CloudWatch resource tagging",
				"Completed CloudWatch resource tagging",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockCloudWatchClient)
			tt.setupMocks(mockClient)

			// Create tagger
			tagger := &AWSResourceTagger{
				ctx:       context.Background(),
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tt.tags,
			}

			// Capture logs
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			defer log.SetOutput(os.Stderr)

			// Execute with mock client
			tagger.tagCloudWatchResourcesWithClient(mockClient)

			// Verify calls
			mockClient.AssertExpectations(t)

			// Verify logs
			logOutput := logBuffer.String()
			for _, expectedLog := range tt.expectLogs {
				assert.Contains(t, logOutput, expectedLog,
					"Expected log message '%s' not found in output:\n%s",
					expectedLog, logOutput)
			}
		})
	}
}

// TestTagCloudWatchResourcesWithTagError tests the specific error case for TagResource
func TestTagCloudWatchResourcesWithTagError(t *testing.T) {
	ctx := context.Background()
	mockClient := new(MockCloudWatchClient)

	// Setup mock for DescribeAlarms
	mockClient.On("DescribeAlarms", mock.Anything, mock.Anything).
		Return(&cloudwatch.DescribeAlarmsOutput{
			MetricAlarms: []cloudwatchtypes.MetricAlarm{
				{
					AlarmName: aws.String("test-alarm"),
					AlarmArn:  aws.String("arn:aws:cloudwatch:us-west-2:123456789012:alarm:test-alarm"),
				},
			},
		}, nil)

	// Setup mock for TagResource to fail
	mockClient.On("TagResource", mock.Anything, mock.Anything).
		Return(nil, &smithy.GenericAPIError{
			Code:    "AccessDenied",
			Message: "User not authorized",
		})

	// Setup mock for ListDashboards
	mockClient.On("ListDashboards", mock.Anything, mock.Anything).
		Return(&cloudwatch.ListDashboardsOutput{
			DashboardEntries: []cloudwatchtypes.DashboardEntry{},
		}, nil)

	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		region:    "us-west-2",
		tags:      map[string]string{"Environment": "Test"},
	}

	var logBuffer bytes.Buffer
	log.SetOutput(&logBuffer)
	defer log.SetOutput(os.Stderr)

	tagger.tagCloudWatchResourcesWithClient(mockClient)

	// Verify error was handled
	mockClient.AssertExpectations(t)
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Access denied")
}
