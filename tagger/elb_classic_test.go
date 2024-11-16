package tagger

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbTypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockClassicELBClient is a mock implementation of ClassicELBAPI
type MockClassicELBClient struct {
	mock.Mock
}

func (m *MockClassicELBClient) DescribeLoadBalancers(ctx context.Context, params *elasticloadbalancing.DescribeLoadBalancersInput, optFns ...func(*elasticloadbalancing.Options)) (*elasticloadbalancing.DescribeLoadBalancersOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elasticloadbalancing.DescribeLoadBalancersOutput), args.Error(1)
}

func (m *MockClassicELBClient) AddTags(ctx context.Context, params *elasticloadbalancing.AddTagsInput, optFns ...func(*elasticloadbalancing.Options)) (*elasticloadbalancing.AddTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elasticloadbalancing.AddTagsOutput), args.Error(1)
}

func TestConvertToClassicELBTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     map[string]string
		expected []elbTypes.Tag
	}{
		{
			name: "Convert multiple tags",
			tags: map[string]string{
				"Environment": "Test",
				"Project":     "Demo",
				"Owner":       "TeamA",
			},
			expected: []elbTypes.Tag{
				{Key: aws.String("Environment"), Value: aws.String("Test")},
				{Key: aws.String("Project"), Value: aws.String("Demo")},
				{Key: aws.String("Owner"), Value: aws.String("TeamA")},
			},
		},
		{
			name:     "Empty tags map",
			tags:     map[string]string{},
			expected: []elbTypes.Tag{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tagger := &AWSResourceTagger{tags: tt.tags}
			result := tagger.convertToClassicELBTags()

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

func TestTagClassicLoadBalancers(t *testing.T) {
	ctx := context.Background()
	validTags := map[string]string{
		"Environment": "Test",
		"Project":     "Demo",
	}

	tests := []struct {
		name        string
		tags        map[string]string
		setupMocks  func(*MockClassicELBClient)
		expectLogs  []string
		skipLogTest bool
	}{
		{
			name: "Successfully tag multiple Classic Load Balancers",
			tags: validTags,
			setupMocks: func(m *MockClassicELBClient) {
				m.On("DescribeLoadBalancers", mock.Anything, &elasticloadbalancing.DescribeLoadBalancersInput{}).
					Return(&elasticloadbalancing.DescribeLoadBalancersOutput{
						LoadBalancerDescriptions: []elbTypes.LoadBalancerDescription{
							{LoadBalancerName: aws.String("classic-lb-1")},
							{LoadBalancerName: aws.String("classic-lb-2")},
						},
					}, nil)

				m.On("AddTags", mock.Anything, mock.MatchedBy(func(input *elasticloadbalancing.AddTagsInput) bool {
					return len(input.LoadBalancerNames) == 1 && len(input.Tags) == 2
				})).Return(&elasticloadbalancing.AddTagsOutput{}, nil).Times(2)
			},
			expectLogs: []string{
				"Successfully tagged Classic Load Balancer: classic-lb-1",
				"Successfully tagged Classic Load Balancer: classic-lb-2",
			},
		},
		{
			name: "Handle DescribeLoadBalancers error",
			tags: validTags,
			setupMocks: func(m *MockClassicELBClient) {
				m.On("DescribeLoadBalancers", mock.Anything, mock.Anything).
					Return((*elasticloadbalancing.DescribeLoadBalancersOutput)(nil), fmt.Errorf("API error"))
			},
			expectLogs: []string{
				"Error tagging Classic Load Balancers resource all: API error",
			},
		},
		{
			name: "Handle AddTags error",
			tags: validTags,
			setupMocks: func(m *MockClassicELBClient) {
				m.On("DescribeLoadBalancers", mock.Anything, mock.Anything).
					Return(&elasticloadbalancing.DescribeLoadBalancersOutput{
						LoadBalancerDescriptions: []elbTypes.LoadBalancerDescription{
							{LoadBalancerName: aws.String("classic-lb-1")},
						},
					}, nil)

				m.On("AddTags", mock.Anything, mock.Anything).
					Return((*elasticloadbalancing.AddTagsOutput)(nil), fmt.Errorf("tagging error"))
			},
			expectLogs: []string{
				"Error tagging Classic Load Balancer resource classic-lb-1: tagging error",
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
			mockClient := new(MockClassicELBClient)
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
			tagger.tagClassicLoadBalancersWithClient(mockClient)

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
