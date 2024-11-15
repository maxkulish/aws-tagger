package tagger

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockAthenaClient is a mock implementation of AthenaAPI
type MockAthenaClient struct {
	mock.Mock
}

func (m *MockAthenaClient) ListWorkGroups(ctx context.Context, params *athena.ListWorkGroupsInput, optFns ...func(*athena.Options)) (*athena.ListWorkGroupsOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.ListWorkGroupsOutput), args.Error(1)
}

func (m *MockAthenaClient) ListDataCatalogs(ctx context.Context, params *athena.ListDataCatalogsInput, optFns ...func(*athena.Options)) (*athena.ListDataCatalogsOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.ListDataCatalogsOutput), args.Error(1)
}

func (m *MockAthenaClient) ListPreparedStatements(ctx context.Context, params *athena.ListPreparedStatementsInput, optFns ...func(*athena.Options)) (*athena.ListPreparedStatementsOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.ListPreparedStatementsOutput), args.Error(1)
}

func (m *MockAthenaClient) ListQueryExecutions(ctx context.Context, params *athena.ListQueryExecutionsInput, optFns ...func(*athena.Options)) (*athena.ListQueryExecutionsOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.ListQueryExecutionsOutput), args.Error(1)
}

func (m *MockAthenaClient) TagResource(ctx context.Context, params *athena.TagResourceInput, optFns ...func(*athena.Options)) (*athena.TagResourceOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.TagResourceOutput), args.Error(1)
}

func TestTagAthenaWorkgroups(t *testing.T) {
	// Setup
	mockClient := new(MockAthenaClient)
	ctx := context.Background()
	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		tags:      map[string]string{"Environment": "Test"},
	}

	tests := []struct {
		name           string
		workgroups     []athenatypes.WorkGroupSummary
		expectedCalls  int
		expectedErrors bool
		setupMocks     func(*MockAthenaClient)
	}{
		{
			name: "Successfully tag multiple workgroups",
			workgroups: []athenatypes.WorkGroupSummary{
				{Name: aws.String("workgroup1")},
				{Name: aws.String("workgroup2")},
			},
			expectedCalls:  2,
			expectedErrors: false,
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", ctx, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
							{Name: aws.String("workgroup2")},
						},
					}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:workgroup/workgroup1")
				})).Return(&athena.TagResourceOutput{}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:workgroup/workgroup2")
				})).Return(&athena.TagResourceOutput{}, nil)
			},
		},
		{
			name: "Skip primary workgroup",
			workgroups: []athenatypes.WorkGroupSummary{
				{Name: aws.String("primary")},
				{Name: aws.String("workgroup1")},
			},
			expectedCalls:  1,
			expectedErrors: false,
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", ctx, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("primary")},
							{Name: aws.String("workgroup1")},
						},
					}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:workgroup/workgroup1")
				})).Return(&athena.TagResourceOutput{}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient = new(MockAthenaClient)
			tt.setupMocks(mockClient)
			tagger.tagAthenaWorkgroups(mockClient)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagAthenaDataCatalogs(t *testing.T) {
	mockClient := new(MockAthenaClient)
	ctx := context.Background()
	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		tags:      map[string]string{"Environment": "Test"},
	}

	tests := []struct {
		name          string
		catalogs      []athenatypes.DataCatalogSummary
		expectedCalls int
		setupMocks    func(*MockAthenaClient)
	}{
		{
			name: "Skip AwsDataCatalog",
			catalogs: []athenatypes.DataCatalogSummary{
				{CatalogName: aws.String("AwsDataCatalog")},
				{CatalogName: aws.String("custom-catalog")},
			},
			expectedCalls: 1,
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListDataCatalogs", ctx, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("AwsDataCatalog")},
							{CatalogName: aws.String("custom-catalog")},
						},
					}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:datacatalog/custom-catalog")
				})).Return(&athena.TagResourceOutput{}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient = new(MockAthenaClient)
			tt.setupMocks(mockClient)
			tagger.tagAthenaDataCatalogs(mockClient)
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConvertToAthenaTags(t *testing.T) {
	tagger := &AWSResourceTagger{
		tags: map[string]string{
			"Environment": "Test",
			"Project":     "Demo",
		},
	}

	athenaTags := tagger.convertToAthenaTags()
	assert.Len(t, athenaTags, 2)

	tagMap := make(map[string]string)
	for _, tag := range athenaTags {
		tagMap[*tag.Key] = *tag.Value
	}

	assert.Equal(t, "Test", tagMap["Environment"])
	assert.Equal(t, "Demo", tagMap["Project"])
}
