package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	ostypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockOpenSearchClient is a mock implementation of OpenSearchAPI
type MockOpenSearchClient struct {
	mock.Mock
}

func (m *MockOpenSearchClient) ListDomainNames(ctx context.Context, params *opensearch.ListDomainNamesInput, optFns ...func(*opensearch.Options)) (*opensearch.ListDomainNamesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*opensearch.ListDomainNamesOutput), args.Error(1)
}

func (m *MockOpenSearchClient) DescribeDomain(ctx context.Context, params *opensearch.DescribeDomainInput, optFns ...func(*opensearch.Options)) (*opensearch.DescribeDomainOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*opensearch.DescribeDomainOutput), args.Error(1)
}

func (m *MockOpenSearchClient) AddTags(ctx context.Context, params *opensearch.AddTagsInput, optFns ...func(*opensearch.Options)) (*opensearch.AddTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*opensearch.AddTagsOutput), args.Error(1)
}

func (m *MockOpenSearchClient) ListTags(ctx context.Context, params *opensearch.ListTagsInput, optFns ...func(*opensearch.Options)) (*opensearch.ListTagsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*opensearch.ListTagsOutput), args.Error(1)
}

func TestFormatTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     []ostypes.Tag
		expected string
	}{
		{
			name: "Multiple tags",
			tags: []ostypes.Tag{
				{Key: aws.String("env"), Value: aws.String("prod")},
				{Key: aws.String("team"), Value: aws.String("platform")},
			},
			expected: "{env: prod, team: platform}",
		},
		{
			name:     "Empty tags",
			tags:     []ostypes.Tag{},
			expected: "{}",
		},
		{
			name: "Nil key or value",
			tags: []ostypes.Tag{
				{Key: aws.String("env"), Value: nil},
				{Key: nil, Value: aws.String("platform")},
				{Key: aws.String("valid"), Value: aws.String("value")},
			},
			expected: "{valid: value}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatTags(tt.tags)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTagOpenSearchResources(t *testing.T) {
	tests := []struct {
		name             string
		domains          []ostypes.DomainInfo
		tags             map[string]string
		listError        error
		describeErrors   map[string]error
		addTagErrors     map[string]error
		listTagErrors    map[string]error
		expectedAddTags  int
		expectedListTags int
	}{
		{
			name: "Successfully tag multiple domains",
			domains: []ostypes.DomainInfo{
				{DomainName: aws.String("domain1")},
				{DomainName: aws.String("domain2")},
			},
			tags: map[string]string{
				"env":  "prod",
				"team": "platform",
			},
			describeErrors:   map[string]error{},
			addTagErrors:     map[string]error{},
			listTagErrors:    map[string]error{},
			expectedAddTags:  2,
			expectedListTags: 2,
		},
		{
			name:             "Handle ListDomainNames error",
			listError:        errors.New("ListDomainNames failed"),
			expectedAddTags:  0,
			expectedListTags: 0,
		},
		{
			name: "Handle DescribeDomain error",
			domains: []ostypes.DomainInfo{
				{DomainName: aws.String("domain1")},
				{DomainName: aws.String("domain2")},
			},
			describeErrors: map[string]error{
				"domain1": errors.New("DescribeDomain failed"),
			},
			expectedAddTags:  1,
			expectedListTags: 1,
		},
		{
			name: "Handle AddTags error",
			domains: []ostypes.DomainInfo{
				{DomainName: aws.String("domain1")},
				{DomainName: aws.String("domain2")},
			},
			addTagErrors: map[string]error{
				"domain1": errors.New("AddTags failed"),
			},
			expectedAddTags:  2,
			expectedListTags: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockOpenSearchClient)

			// Setup ListDomainNames mock
			listDomainsOutput := &opensearch.ListDomainNamesOutput{DomainNames: tt.domains}
			mockClient.On("ListDomainNames", mock.Anything, mock.Anything).Return(listDomainsOutput, tt.listError)

			// Setup mocks for each domain
			for _, domain := range tt.domains {
				domainName := aws.ToString(domain.DomainName)

				// DescribeDomain mock
				describeOutput := &opensearch.DescribeDomainOutput{
					DomainStatus: &ostypes.DomainStatus{
						ARN:        aws.String("arn:aws:opensearch:" + domainName),
						DomainName: domain.DomainName,
					},
				}
				mockClient.On("DescribeDomain", mock.Anything, &opensearch.DescribeDomainInput{
					DomainName: domain.DomainName,
				}).Return(describeOutput, tt.describeErrors[domainName])

				// AddTags mock
				addTagsOutput := &opensearch.AddTagsOutput{}
				mockClient.On("AddTags", mock.Anything, mock.Anything).Return(addTagsOutput, tt.addTagErrors[domainName])

				// ListTags mock
				listTagsOutput := &opensearch.ListTagsOutput{
					TagList: convertToOpenSearchTags(tt.tags),
				}
				mockClient.On("ListTags", mock.Anything, mock.Anything).Return(listTagsOutput, tt.listTagErrors[domainName])
			}

			// Create tagger instance
			tagger := &AWSResourceTagger{
				ctx:  context.Background(),
				tags: tt.tags,
			}

			// Execute
			tagger.tagOpenSearchResourcesWithClient(mockClient)

			// Verify expectations
			mockClient.AssertNumberOfCalls(t, "AddTags", tt.expectedAddTags)
			mockClient.AssertNumberOfCalls(t, "ListTags", tt.expectedListTags)
			mockClient.AssertExpectations(t)
		})
	}
}
