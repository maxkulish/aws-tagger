package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/mock"
)

// MockEC2Client is a mock implementation of EC2API
type MockEC2Client struct {
	mock.Mock
}

func (m *MockEC2Client) DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeInstancesOutput), args.Error(1)
}

func (m *MockEC2Client) DescribeVolumes(ctx context.Context, params *ec2.DescribeVolumesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeVolumesOutput), args.Error(1)
}

func (m *MockEC2Client) CreateTags(ctx context.Context, params *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.CreateTagsOutput), args.Error(1)
}

func TestTagEC2Resources(t *testing.T) {
	tests := []struct {
		name        string
		setupMocks  func(*MockEC2Client)
		expectedErr bool
	}{
		{
			name: "Successfully tag all resources",
			setupMocks: func(m *MockEC2Client) {
				// Single page response for instances
				m.On("DescribeInstances", mock.Anything, mock.Anything).
					Return(&ec2.DescribeInstancesOutput{
						Reservations: []ec2types.Reservation{
							{
								Instances: []ec2types.Instance{
									{InstanceId: aws.String("i-1234567890abcdef0")},
								},
							},
						},
					}, nil).Once()

				// Mock CreateTags for instance
				m.On("CreateTags", mock.Anything, mock.MatchedBy(func(input *ec2.CreateTagsInput) bool {
					return input.Resources[0] == "i-1234567890abcdef0"
				})).Return(&ec2.CreateTagsOutput{}, nil).Once()

				// Single page response for volumes
				m.On("DescribeVolumes", mock.Anything, mock.Anything).
					Return(&ec2.DescribeVolumesOutput{
						Volumes: []ec2types.Volume{
							{VolumeId: aws.String("vol-1234567890abcdef0")},
						},
					}, nil).Once()

				// Mock CreateTags for volume
				m.On("CreateTags", mock.Anything, mock.MatchedBy(func(input *ec2.CreateTagsInput) bool {
					return input.Resources[0] == "vol-1234567890abcdef0"
				})).Return(&ec2.CreateTagsOutput{}, nil).Once()
			},
			expectedErr: false,
		},
		{
			name: "Handle DescribeInstances error",
			setupMocks: func(m *MockEC2Client) {
				m.On("DescribeInstances", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error")).Once()
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockEC2Client)
			tt.setupMocks(mockClient)

			tagger := &AWSResourceTagger{
				ctx:     context.Background(),
				cfg:     aws.Config{Region: "us-east-1"},
				awsTags: []ec2types.Tag{{Key: aws.String("Environment"), Value: aws.String("Test")}},
			}

			tagger.tagEC2ResourcesWithClient(mockClient)

			if tt.expectedErr {
				mockClient.AssertCalled(t, "DescribeInstances", mock.Anything, mock.Anything)
			} else {
				mockClient.AssertExpectations(t)
			}

			mockClient.AssertExpectations(t)
		})
	}
}
