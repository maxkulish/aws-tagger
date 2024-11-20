package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticeTypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/mock"
)

// MockVPCClient is a mock implementation of EC2 client for VPC resources
type MockVPCClient struct {
	mock.Mock
}

func (m *MockVPCClient) DescribeTransitGateways(ctx context.Context, params *ec2.DescribeTransitGatewaysInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewaysOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeTransitGatewaysOutput), args.Error(1)
}

func (m *MockVPCClient) DescribeTransitGatewayAttachments(ctx context.Context, params *ec2.DescribeTransitGatewayAttachmentsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewayAttachmentsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeTransitGatewayAttachmentsOutput), args.Error(1)
}

func (m *MockVPCClient) DescribeTransitGatewayPeeringAttachments(ctx context.Context, params *ec2.DescribeTransitGatewayPeeringAttachmentsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewayPeeringAttachmentsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.DescribeTransitGatewayPeeringAttachmentsOutput), args.Error(1)
}

func (m *MockVPCClient) CreateTags(ctx context.Context, params *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ec2.CreateTagsOutput), args.Error(1)
}

// MockVPCLatticeClient is a mock implementation of VPC Lattice client
type MockVPCLatticeClient struct {
	mock.Mock
}

func (m *MockVPCLatticeClient) ListServiceNetworks(ctx context.Context, params *vpclattice.ListServiceNetworksInput, optFns ...func(*vpclattice.Options)) (*vpclattice.ListServiceNetworksOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*vpclattice.ListServiceNetworksOutput), args.Error(1)
}

func (m *MockVPCLatticeClient) ListServices(ctx context.Context, params *vpclattice.ListServicesInput, optFns ...func(*vpclattice.Options)) (*vpclattice.ListServicesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*vpclattice.ListServicesOutput), args.Error(1)
}

func (m *MockVPCLatticeClient) TagResource(ctx context.Context, params *vpclattice.TagResourceInput, optFns ...func(*vpclattice.Options)) (*vpclattice.TagResourceOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*vpclattice.TagResourceOutput), args.Error(1)
}

func TestTagVPCResourcesWithClients(t *testing.T) {
	tests := []struct {
		name              string
		setupEC2Mocks     func(*MockVPCClient)
		setupLatticeMocks func(*MockVPCLatticeClient)
		expectError       bool
		tags              map[string]string
	}{
		{
			name: "Successfully tag all VPC resources",
			tags: map[string]string{"Environment": "Test"},
			setupEC2Mocks: func(m *MockVPCClient) {
				// Setup Transit Gateway
				m.On("DescribeTransitGateways", mock.Anything, mock.Anything).
					Return(&ec2.DescribeTransitGatewaysOutput{
						TransitGateways: []types.TransitGateway{
							{
								TransitGatewayId: aws.String("tgw-123"),
								State:            types.TransitGatewayStateAvailable,
							},
						},
					}, nil)

				// Setup DescribeTransitGatewayAttachments for VPN
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					// Check transit gateway ID filter
					if *input.Filters[0].Name != "transit-gateway-id" || input.Filters[0].Values[0] != "tgw-123" {
						return false
					}
					// Check resource type filter
					return *input.Filters[1].Name == "resource-type" && input.Filters[1].Values[0] == "vpn"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-vpn-123"),
							TransitGatewayId:           aws.String("tgw-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeVpn,
						},
					},
				}, nil)

				// Setup DescribeTransitGatewayAttachments for VPC
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					// Check transit gateway ID filter
					if *input.Filters[0].Name != "transit-gateway-id" || input.Filters[0].Values[0] != "tgw-123" {
						return false
					}
					// Check resource type filter
					return *input.Filters[1].Name == "resource-type" && input.Filters[1].Values[0] == "vpc"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-vpc-123"),
							TransitGatewayId:           aws.String("tgw-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeVpc,
						},
					},
				}, nil)

				// Add similar mock for DirectConnect attachments
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					// Check transit gateway ID filter
					if *input.Filters[0].Name != "transit-gateway-id" || input.Filters[0].Values[0] != "tgw-123" {
						return false
					}
					// Check resource type filter
					return *input.Filters[1].Name == "resource-type" && input.Filters[1].Values[0] == "direct-connect-gateway"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-dx-123"),
							TransitGatewayId:           aws.String("tgw-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeDirectConnectGateway,
						},
					},
				}, nil)

				// Setup Peering Attachments
				m.On("DescribeTransitGatewayPeeringAttachments", mock.Anything, mock.Anything).
					Return(&ec2.DescribeTransitGatewayPeeringAttachmentsOutput{
						TransitGatewayPeeringAttachments: []types.TransitGatewayPeeringAttachment{
							{
								TransitGatewayAttachmentId: aws.String("tgw-attach-peer-123"),
								State:                      types.TransitGatewayAttachmentStateAvailable,
							},
						},
					}, nil)

				// Setup CreateTags for all resources
				m.On("CreateTags", mock.Anything, mock.Anything).Return(&ec2.CreateTagsOutput{}, nil)
			},
			setupLatticeMocks: func(m *MockVPCLatticeClient) {
				m.On("ListServiceNetworks", mock.Anything, mock.Anything).
					Return(&vpclattice.ListServiceNetworksOutput{
						Items: []vpclatticeTypes.ServiceNetworkSummary{
							{
								Name: aws.String("network1"),
								Arn:  aws.String("arn:aws:vpclattice:region:account:servicenetwork/network1"),
							},
						},
					}, nil)

				m.On("ListServices", mock.Anything, mock.Anything).
					Return(&vpclattice.ListServicesOutput{
						Items: []vpclatticeTypes.ServiceSummary{
							{
								Name: aws.String("service1"),
								Arn:  aws.String("arn:aws:vpclattice:region:account:service/service1"),
							},
						},
					}, nil)

				m.On("TagResource", mock.Anything, mock.Anything).Return(&vpclattice.TagResourceOutput{}, nil)
			},
		},
		{
			name: "Handle Transit Gateway API error",
			tags: map[string]string{"Environment": "Test"},
			setupEC2Mocks: func(m *MockVPCClient) {
				m.On("DescribeTransitGateways", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
			},
			setupLatticeMocks: func(m *MockVPCLatticeClient) {
				m.On("ListServiceNetworks", mock.Anything, mock.Anything).
					Return(&vpclattice.ListServiceNetworksOutput{}, nil)
				m.On("ListServices", mock.Anything, mock.Anything).
					Return(&vpclattice.ListServicesOutput{}, nil)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEC2Client := new(MockVPCClient)
			mockLatticeClient := new(MockVPCLatticeClient)

			tt.setupEC2Mocks(mockEC2Client)
			tt.setupLatticeMocks(mockLatticeClient)

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			tagger.tagVPCResourcesWithClients(mockEC2Client, mockLatticeClient)

			mockEC2Client.AssertExpectations(t)
			mockLatticeClient.AssertExpectations(t)
		})
	}
}

func TestTagTransitGatewayResources(t *testing.T) {
	tests := []struct {
		name          string
		tgws          []types.TransitGateway
		attachments   map[string][]types.TransitGatewayAttachment
		peeringAttach []types.TransitGatewayPeeringAttachment
		expectError   bool
		setupMocks    func(*MockVPCClient)
	}{
		{
			name: "Successfully tag transit gateway and attachments",
			tgws: []types.TransitGateway{
				{
					TransitGatewayId: aws.String("tgw-123"),
					State:            types.TransitGatewayStateAvailable,
				},
			},
			attachments: map[string][]types.TransitGatewayAttachment{
				"vpn": {
					{
						TransitGatewayAttachmentId: aws.String("tgw-attach-vpn-123"),
						TransitGatewayId:           aws.String("tgw-123"),
						ResourceType:               types.TransitGatewayAttachmentResourceTypeVpn,
					},
				},
				"vpc": {
					{
						TransitGatewayAttachmentId: aws.String("tgw-attach-vpc-123"),
						TransitGatewayId:           aws.String("tgw-123"),
						ResourceType:               types.TransitGatewayAttachmentResourceTypeVpc,
					},
				},
			},
			peeringAttach: []types.TransitGatewayPeeringAttachment{
				{
					TransitGatewayAttachmentId: aws.String("tgw-attach-peer-123"),
					State:                      types.TransitGatewayAttachmentStateAvailable,
				},
			},
			setupMocks: func(m *MockVPCClient) {
				// Setup DescribeTransitGateways
				m.On("DescribeTransitGateways", mock.Anything, mock.Anything).
					Return(&ec2.DescribeTransitGatewaysOutput{
						TransitGateways: []types.TransitGateway{
							{
								TransitGatewayId: aws.String("tgw-123"),
								State:            types.TransitGatewayStateAvailable,
							},
						},
					}, nil)

				// Setup CreateTags for various resources
				m.On("CreateTags", mock.Anything, mock.Anything).Return(&ec2.CreateTagsOutput{}, nil)

				// Setup DescribeTransitGatewayAttachments for Direct Connect
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					resourceTypeFilter := input.Filters[1]
					return len(resourceTypeFilter.Values) > 0 && resourceTypeFilter.Values[0] == "direct-connect-gateway"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-dx-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeDirectConnectGateway,
						},
					},
				}, nil)

				// Setup DescribeTransitGatewayAttachments for VPC
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					resourceTypeFilter := input.Filters[1]
					return len(resourceTypeFilter.Values) > 0 && resourceTypeFilter.Values[0] == "vpc"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-vpc-123"),
							TransitGatewayId:           aws.String("tgw-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeVpc,
						},
					},
				}, nil)

				// Setup DescribeTransitGatewayAttachments for VPN
				m.On("DescribeTransitGatewayAttachments", mock.Anything, mock.MatchedBy(func(input *ec2.DescribeTransitGatewayAttachmentsInput) bool {
					if len(input.Filters) != 2 {
						return false
					}
					resourceTypeFilter := input.Filters[1]
					return len(resourceTypeFilter.Values) > 0 && resourceTypeFilter.Values[0] == "vpn"
				})).Return(&ec2.DescribeTransitGatewayAttachmentsOutput{
					TransitGatewayAttachments: []types.TransitGatewayAttachment{
						{
							TransitGatewayAttachmentId: aws.String("tgw-attach-vpn-123"),
							ResourceType:               types.TransitGatewayAttachmentResourceTypeVpn,
						},
					},
				}, nil)

				// Setup DescribeTransitGatewayPeeringAttachments
				m.On("DescribeTransitGatewayPeeringAttachments", mock.Anything, mock.Anything).
					Return(&ec2.DescribeTransitGatewayPeeringAttachmentsOutput{
						TransitGatewayPeeringAttachments: []types.TransitGatewayPeeringAttachment{
							{
								TransitGatewayAttachmentId: aws.String("tgw-attach-peer-123"),
								State:                      types.TransitGatewayAttachmentStateAvailable,
							},
						},
					}, nil)
			},
		},
		{
			name:        "Handle DescribeTransitGateways error",
			expectError: true,
			setupMocks: func(m *MockVPCClient) {
				m.On("DescribeTransitGateways", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockVPCClient)
			tt.setupMocks(mockClient)

			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: map[string]string{"Environment": "Test"},
			}

			tagger.tagTransitGatewayResourcesWithClient(mockClient)

			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagVPCLatticeResources(t *testing.T) {
	tests := []struct {
		name        string
		networks    []vpclatticeTypes.ServiceNetworkSummary
		services    []vpclatticeTypes.ServiceSummary
		expectError bool
		setupMocks  func(*MockVPCLatticeClient)
	}{
		{
			name: "Successfully tag networks and services",
			setupMocks: func(m *MockVPCLatticeClient) {
				m.On("ListServiceNetworks", mock.Anything, &vpclattice.ListServiceNetworksInput{}).
					Return(&vpclattice.ListServiceNetworksOutput{
						Items: []vpclatticeTypes.ServiceNetworkSummary{
							{
								Name: aws.String("network1"),
								Arn:  aws.String("arn:aws:vpclattice:region:account:servicenetwork/network1"),
							},
						},
					}, nil)

				m.On("ListServices", mock.Anything, &vpclattice.ListServicesInput{}).
					Return(&vpclattice.ListServicesOutput{
						Items: []vpclatticeTypes.ServiceSummary{
							{
								Name: aws.String("service1"),
								Arn:  aws.String("arn:aws:vpclattice:region:account:service/service1"),
							},
						},
					}, nil)

				m.On("TagResource", mock.Anything, mock.Anything).
					Return(&vpclattice.TagResourceOutput{}, nil)
			},
		},
		{
			name:        "Handle ListServiceNetworks error",
			expectError: true,
			setupMocks: func(m *MockVPCLatticeClient) {
				m.On("ListServiceNetworks", mock.Anything, &vpclattice.ListServiceNetworksInput{}).
					Return(nil, errors.New("API error"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockVPCLatticeClient)
			tt.setupMocks(mockClient)

			// Create tagger with proper AWS configuration
			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				cfg:  aws.Config{Region: "us-west-2"},
				tags: map[string]string{"Environment": "Test"},
			}

			// Call the client version of the method instead
			tagger.tagVPCLatticeResourcesWithClient(mockClient)

			mockClient.AssertExpectations(t)
		})
	}
}
