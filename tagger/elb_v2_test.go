package tagger

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2Types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockELBv2Client is a mock implementation of ELBv2API
type MockELBv2Client struct {
	mock.Mock
}

func (m *MockELBv2Client) DescribeLoadBalancers(ctx context.Context, params *elasticloadbalancingv2.DescribeLoadBalancersInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.DescribeLoadBalancersOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elasticloadbalancingv2.DescribeLoadBalancersOutput), args.Error(1)
}

func (m *MockELBv2Client) DescribeTargetGroups(ctx context.Context, params *elasticloadbalancingv2.DescribeTargetGroupsInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.DescribeTargetGroupsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elasticloadbalancingv2.DescribeTargetGroupsOutput), args.Error(1)
}

func (m *MockELBv2Client) AddTags(ctx context.Context, params *elasticloadbalancingv2.AddTagsInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.AddTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elasticloadbalancingv2.AddTagsOutput), args.Error(1)
}

func TestConvertToELBv2Tags(t *testing.T) {
	tests := []struct {
		name     string
		tags     map[string]string
		expected []elbv2Types.Tag
	}{
		{
			name: "Convert multiple tags",
			tags: map[string]string{
				"Environment": "Test",
				"Project":     "Demo",
				"Owner":       "TeamA",
			},
			expected: []elbv2Types.Tag{
				{Key: aws.String("Environment"), Value: aws.String("Test")},
				{Key: aws.String("Project"), Value: aws.String("Demo")},
				{Key: aws.String("Owner"), Value: aws.String("TeamA")},
			},
		},
		{
			name:     "Empty tags map",
			tags:     map[string]string{},
			expected: []elbv2Types.Tag{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tagger := &AWSResourceTagger{tags: tt.tags}
			result := tagger.convertToELBv2Tags()

			// Convert to maps for easier comparison
			resultMap := make(map[string]string)
			for _, tag := range result {
				resultMap[*tag.Key] = *tag.Value
			}

			expectedMap := make(map[string]string)
			for _, tag := range tt.expected {
				expectedMap[*tag.Key] = *tag.Value
			}

			assert.Equal(t, expectedMap, resultMap)
			assert.Equal(t, len(tt.expected), len(result))
		})
	}
}

func TestTagApplicationAndNetworkLoadBalancers(t *testing.T) {
	ctx := context.Background()
	validTags := map[string]string{
		"Environment": "Test",
		"Project":     "Demo",
	}

	tests := []struct {
		name        string
		tags        map[string]string
		setupMocks  func(*MockELBv2Client)
		expectLogs  []string
		skipLogTest bool
	}{
		{
			name: "Successfully tag ALB/NLB and target groups",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				// Mock describing load balancers
				m.On("DescribeLoadBalancers", mock.Anything, &elasticloadbalancingv2.DescribeLoadBalancersInput{}).
					Return(&elasticloadbalancingv2.DescribeLoadBalancersOutput{
						LoadBalancers: []elbv2Types.LoadBalancer{
							{
								LoadBalancerArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/alb-1"),
								LoadBalancerName: aws.String("alb-1"),
								Type:             elbv2Types.LoadBalancerTypeEnumApplication,
							},
							{
								LoadBalancerArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/net/nlb-1"),
								LoadBalancerName: aws.String("nlb-1"),
								Type:             elbv2Types.LoadBalancerTypeEnumNetwork,
							},
						},
					}, nil)

				// Mock describing target groups for each LB
				m.On("DescribeTargetGroups", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancingv2.DescribeTargetGroupsInput) bool {
					return aws.ToString(input.LoadBalancerArn) != ""
				})).Return(&elasticloadbalancingv2.DescribeTargetGroupsOutput{
					TargetGroups: []elbv2Types.TargetGroup{
						{
							TargetGroupArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/tg-1"),
							TargetGroupName: aws.String("tg-1"),
						},
					},
				}, nil)

				// Mock AddTags for both LBs and target groups
				m.On("AddTags", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancingv2.AddTagsInput) bool {
					return len(input.ResourceArns) == 1 && len(input.Tags) == 2
				})).Return(&elasticloadbalancingv2.AddTagsOutput{}, nil)
			},
			expectLogs: []string{
				"Successfully tagged application Load Balancer: alb-1",
				"Successfully tagged network Load Balancer: nlb-1",
				"Successfully tagged Target Group: tg-1",
			},
		},
		{
			name: "Handle DescribeLoadBalancers error",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				m.On("DescribeLoadBalancers", mock.Anything, mock.Anything).
					Return((*elasticloadbalancingv2.DescribeLoadBalancersOutput)(nil), fmt.Errorf("API error"))
			},
			expectLogs: []string{
				"Error tagging ALB/NLB Load Balancers resource all: API error",
			},
		},
		{
			name: "Handle AddTags error for load balancer",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				lbArn := "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/alb-1"

				// Mock DescribeLoadBalancers
				m.On("DescribeLoadBalancers", mock.Anything, mock.Anything).
					Return(&elasticloadbalancingv2.DescribeLoadBalancersOutput{
						LoadBalancers: []elbv2Types.LoadBalancer{
							{
								LoadBalancerArn:  aws.String(lbArn),
								LoadBalancerName: aws.String("alb-1"),
								Type:             elbv2Types.LoadBalancerTypeEnumApplication,
							},
						},
					}, nil)

				// Mock AddTags to fail
				m.On("AddTags", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancingv2.AddTagsInput) bool {
					return len(input.ResourceArns) == 1 && input.ResourceArns[0] == lbArn
				})).Return((*elasticloadbalancingv2.AddTagsOutput)(nil), fmt.Errorf("tagging error"))

				// Since tagging LB fails, we shouldn't proceed to target groups
				// No need to mock DescribeTargetGroups
			},
			expectLogs: []string{
				"Error tagging ALB/NLB Load Balancer resource alb-1: tagging error",
			},
		},
		{
			name: "Handle DescribeTargetGroups error",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				lbArn := "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/alb-1"

				// Return one load balancer
				m.On("DescribeLoadBalancers", mock.Anything, mock.Anything).
					Return(&elasticloadbalancingv2.DescribeLoadBalancersOutput{
						LoadBalancers: []elbv2Types.LoadBalancer{
							{
								LoadBalancerArn:  aws.String(lbArn),
								LoadBalancerName: aws.String("alb-1"),
								Type:             elbv2Types.LoadBalancerTypeEnumApplication,
							},
						},
					}, nil)

				// Successfully tag the load balancer
				m.On("AddTags", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancingv2.AddTagsInput) bool {
					return len(input.ResourceArns) == 1 && input.ResourceArns[0] == lbArn
				})).Return(&elasticloadbalancingv2.AddTagsOutput{}, nil)

				// Fail DescribeTargetGroups
				m.On("DescribeTargetGroups", mock.Anything, mock.Anything).
					Return((*elasticloadbalancingv2.DescribeTargetGroupsOutput)(nil), fmt.Errorf("API error"))
			},
			expectLogs: []string{
				"Successfully tagged application Load Balancer: alb-1",
				"Error tagging Target Groups resource",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture log output
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			defer log.SetOutput(os.Stderr)

			// Create mock client and set up expectations
			mockClient := new(MockELBv2Client)
			tt.setupMocks(mockClient)

			// Create tagger with test configuration
			tagger := &AWSResourceTagger{
				ctx:       ctx,
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tt.tags,
			}

			// Execute tagging
			tagger.tagApplicationAndNetworkLoadBalancersWithClient(mockClient)

			// Verify mock expectations
			mockClient.AssertExpectations(t)

			// Verify logs if expected
			if !tt.skipLogTest {
				logOutput := logBuffer.String()
				for _, expectedLog := range tt.expectLogs {
					assert.Contains(t, logOutput, expectedLog)
				}
			}
		})
	}
}

func TestTagTargetGroups(t *testing.T) {
	ctx := context.Background()
	validTags := map[string]string{
		"Environment": "Test",
		"Project":     "Demo",
	}
	lbArn := "arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/test-alb"

	tests := []struct {
		name       string
		tags       map[string]string
		setupMocks func(*MockELBv2Client)
		expectLogs []string
	}{
		{
			name: "Successfully tag multiple target groups",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				m.On("DescribeTargetGroups", mock.Anything, &elasticloadbalancingv2.DescribeTargetGroupsInput{
					LoadBalancerArn: aws.String(lbArn),
				}).Return(&elasticloadbalancingv2.DescribeTargetGroupsOutput{
					TargetGroups: []elbv2Types.TargetGroup{
						{
							TargetGroupArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/tg-1"),
							TargetGroupName: aws.String("tg-1"),
						},
						{
							TargetGroupArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/tg-2"),
							TargetGroupName: aws.String("tg-2"),
						},
					},
				}, nil)

				m.On("AddTags", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancingv2.AddTagsInput) bool {
					return len(input.ResourceArns) == 1 && len(input.Tags) == 2
				})).Return(&elasticloadbalancingv2.AddTagsOutput{}, nil).Times(2)
			},
			expectLogs: []string{
				"Successfully tagged Target Group: tg-1",
				"Successfully tagged Target Group: tg-2",
			},
		},
		{
			name: "Handle DescribeTargetGroups error",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				m.On("DescribeTargetGroups", mock.Anything, mock.Anything).
					Return((*elasticloadbalancingv2.DescribeTargetGroupsOutput)(nil), fmt.Errorf("API error"))
			},
			expectLogs: []string{
				"Error tagging Target Groups resource arn:aws:elasticloadbalancing:us-west-2:123456789012:loadbalancer/app/test-alb: API error",
			},
		},
		{
			name: "Handle AddTags error",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				m.On("DescribeTargetGroups", mock.Anything, mock.Anything).
					Return(&elasticloadbalancingv2.DescribeTargetGroupsOutput{
						TargetGroups: []elbv2Types.TargetGroup{
							{
								TargetGroupArn:  aws.String("arn:aws:elasticloadbalancing:us-west-2:123456789012:targetgroup/tg-1"),
								TargetGroupName: aws.String("tg-1"),
							},
						},
					}, nil)

				m.On("AddTags", mock.Anything, mock.Anything).
					Return((*elasticloadbalancingv2.AddTagsOutput)(nil), fmt.Errorf("tagging error"))
			},
			expectLogs: []string{
				"Error tagging Target Group resource tg-1: tagging error",
			},
		},
		{
			name: "Empty target groups list",
			tags: validTags,
			setupMocks: func(m *MockELBv2Client) {
				m.On("DescribeTargetGroups", mock.Anything, mock.Anything).
					Return(&elasticloadbalancingv2.DescribeTargetGroupsOutput{
						TargetGroups: []elbv2Types.TargetGroup{},
					}, nil)
			},
			expectLogs: []string{}, // No logs expected since no target groups to tag
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture log output
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			defer log.SetOutput(os.Stderr)

			// Create mock client and set up expectations
			mockClient := new(MockELBv2Client)
			tt.setupMocks(mockClient)

			// Create tagger with test configuration
			tagger := &AWSResourceTagger{
				ctx:       ctx,
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tt.tags,
			}

			// Execute tagging
			tagger.tagTargetGroupsWithClient(mockClient, lbArn)

			// Verify mock expectations
			mockClient.AssertExpectations(t)

			// Verify logs
			logOutput := logBuffer.String()
			for _, expectedLog := range tt.expectLogs {
				assert.Contains(t, logOutput, expectedLog)
			}
		})
	}
}
