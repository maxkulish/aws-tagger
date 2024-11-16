package tagger

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/athena"
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

func (m *MockAthenaClient) TagResource(ctx context.Context, params *athena.TagResourceInput, optFns ...func(*athena.Options)) (*athena.TagResourceOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*athena.TagResourceOutput), args.Error(1)
}

func TestValidateTags(t *testing.T) {
	tests := []struct {
		name        string
		tags        map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid tags",
			tags: map[string]string{
				"Environment": "Test",
				"Project":     "Demo",
			},
			expectError: false,
		},
		{
			name: "Too many tags",
			tags: func() map[string]string {
				tags := make(map[string]string)
				for i := 0; i < 51; i++ {
					tags[fmt.Sprintf("key%d", i)] = "value"
				}
				return tags
			}(),
			expectError: true,
			errorMsg:    "number of tags exceeds maximum limit of 50",
		},
		{
			name: "Invalid aws: prefix",
			tags: map[string]string{
				"aws:restricted": "value",
			},
			expectError: true,
			errorMsg:    "tag key cannot start with 'aws:'",
		},
		{
			name: "Key too long",
			tags: map[string]string{
				string(make([]byte, 129)): "value",
			},
			expectError: true,
			errorMsg:    "tag key length must be between 1 and 128 characters",
		},
		{
			name: "Value too long",
			tags: map[string]string{
				"key": string(make([]byte, 257)),
			},
			expectError: true,
			errorMsg:    "tag value length must not exceed 256 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tagger := &AWSResourceTagger{
				tags: tt.tags,
			}
			err := tagger.validateTags()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTagAthenaWorkgroups(t *testing.T) {
	ctx := context.Background()
	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		tags:      map[string]string{"Environment": "Test"},
	}

	tests := []struct {
		name          string
		workgroups    []athenatypes.WorkGroupSummary
		setupMocks    func(*MockAthenaClient)
		expectError   bool
		errorContains string
	}{
		{
			name: "Successfully tag multiple workgroups",
			workgroups: []athenatypes.WorkGroupSummary{
				{Name: aws.String("workgroup1")},
				{Name: aws.String("workgroup2")},
			},
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", ctx, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
							{Name: aws.String("workgroup2")},
						},
					}, nil)

				expectedTags := []athenatypes.Tag{
					{
						Key:   aws.String("Environment"),
						Value: aws.String("Test"),
					},
				}

				// Use mock.MatchedBy to compare the important parts of the input
				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "workgroup/workgroup1") &&
						len(input.Tags) == len(expectedTags) &&
						aws.ToString(input.Tags[0].Key) == aws.ToString(expectedTags[0].Key) &&
						aws.ToString(input.Tags[0].Value) == aws.ToString(expectedTags[0].Value)
				})).Return(&athena.TagResourceOutput{}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "workgroup/workgroup2") &&
						len(input.Tags) == len(expectedTags) &&
						aws.ToString(input.Tags[0].Key) == aws.ToString(expectedTags[0].Key) &&
						aws.ToString(input.Tags[0].Value) == aws.ToString(expectedTags[0].Value)
				})).Return(&athena.TagResourceOutput{}, nil)
			},
			expectError: false,
		},
		{
			name: "Handle ListWorkGroups error",
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", ctx, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{}, fmt.Errorf("API error"))
			},
			expectError:   true,
			errorContains: "failed to list workgroups",
		},
		{
			name: "Handle TagResource error",
			workgroups: []athenatypes.WorkGroupSummary{
				{Name: aws.String("workgroup1")},
			},
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", ctx, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
						},
					}, nil)

				// Use mock.MatchedBy for the error case as well
				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "workgroup/workgroup1")
				})).Return(&athena.TagResourceOutput{}, fmt.Errorf("tagging error"))
			},
			expectError: false, // We don't return error for individual tagging failures
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockAthenaClient)
			tt.setupMocks(mockClient)

			err := tagger.tagAthenaWorkgroups(mockClient)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagAthenaDataCatalogs(t *testing.T) {
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
		setupMocks    func(*MockAthenaClient)
		expectError   bool
		errorContains string
	}{
		{
			name: "Successfully tag custom catalogs",
			catalogs: []athenatypes.DataCatalogSummary{
				{CatalogName: aws.String("custom-catalog")},
			},
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListDataCatalogs", ctx, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog")},
						},
					}, nil)

				expectedTags := []athenatypes.Tag{
					{
						Key:   aws.String("Environment"),
						Value: aws.String("Test"),
					},
				}

				// Use mock.MatchedBy to compare the important parts of the input
				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "datacatalog/custom-catalog") &&
						len(input.Tags) == len(expectedTags) &&
						aws.ToString(input.Tags[0].Key) == aws.ToString(expectedTags[0].Key) &&
						aws.ToString(input.Tags[0].Value) == aws.ToString(expectedTags[0].Value)
				})).Return(&athena.TagResourceOutput{}, nil)
			},
			expectError: false,
		},
		{
			name: "Handle ListDataCatalogs error",
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListDataCatalogs", ctx, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{}, fmt.Errorf("API error"))
			},
			expectError:   true,
			errorContains: "failed to list data catalogs",
		},
		{
			name: "Handle TagResource error",
			catalogs: []athenatypes.DataCatalogSummary{
				{CatalogName: aws.String("custom-catalog")},
			},
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListDataCatalogs", ctx, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog")},
						},
					}, nil)

				// Use mock.MatchedBy for the error case
				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "datacatalog/custom-catalog")
				})).Return(&athena.TagResourceOutput{}, fmt.Errorf("tagging error"))
			},
			expectError: false, // We don't return error for individual tagging failures
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockAthenaClient)
			tt.setupMocks(mockClient)

			err := tagger.tagAthenaDataCatalogs(mockClient)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagResource(t *testing.T) {
	ctx := context.Background()
	tagger := &AWSResourceTagger{
		ctx:       ctx,
		cfg:       aws.Config{Region: "us-west-2"},
		accountID: "123456789012",
		tags:      map[string]string{"Environment": "Test"},
	}

	tests := []struct {
		name          string
		arn           string
		resourceName  string
		resourceType  string
		setupMocks    func(*MockAthenaClient)
		expectError   bool
		errorContains string
	}{
		{
			name:         "Successfully tag resource",
			arn:          "arn:aws:athena:us-west-2:123456789012:workgroup/test",
			resourceName: "test",
			resourceType: "workgroup",
			setupMocks: func(m *MockAthenaClient) {
				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == "arn:aws:athena:us-west-2:123456789012:workgroup/test"
				})).Return(&athena.TagResourceOutput{}, nil)
			},
			expectError: false,
		},
		{
			name:         "Handle tagging error",
			arn:          "arn:aws:athena:us-west-2:123456789012:workgroup/test",
			resourceName: "test",
			resourceType: "workgroup",
			setupMocks: func(m *MockAthenaClient) {
				m.On("TagResource", ctx, mock.Anything).
					Return(&athena.TagResourceOutput{}, fmt.Errorf("tagging error"))
			},
			expectError:   true,
			errorContains: "tagging error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := new(MockAthenaClient)
			tt.setupMocks(mockClient)

			err := tagger.tagResource(mockClient, tt.arn, tt.resourceName, tt.resourceType)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
			mockClient.AssertExpectations(t)
		})
	}
}

func TestConvertToAthenaTags(t *testing.T) {
	tests := []struct {
		name     string
		tags     map[string]string
		expected []athenatypes.Tag
	}{
		{
			name: "Convert multiple tags",
			tags: map[string]string{
				"Environment": "Test",
				"Project":     "Demo",
				"Owner":       "TeamA",
			},
			expected: []athenatypes.Tag{
				{Key: aws.String("Environment"), Value: aws.String("Test")},
				{Key: aws.String("Project"), Value: aws.String("Demo")},
				{Key: aws.String("Owner"), Value: aws.String("TeamA")},
			},
		},
		{
			name:     "Empty tags map",
			tags:     map[string]string{},
			expected: []athenatypes.Tag{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tagger := &AWSResourceTagger{
				tags: tt.tags,
			}

			result := tagger.convertToAthenaTags()

			// Convert result to map for easier comparison
			resultMap := make(map[string]string)
			for _, tag := range result {
				resultMap[*tag.Key] = *tag.Value
			}

			// Convert expected to map
			expectedMap := make(map[string]string)
			for _, tag := range tt.expected {
				expectedMap[*tag.Key] = *tag.Value
			}

			assert.Equal(t, expectedMap, resultMap)
			assert.Equal(t, len(tt.expected), len(result))
		})
	}
}

func TestTagAthenaResources(t *testing.T) {
	ctx := context.Background()
	validTags := map[string]string{
		"Environment": "Test",
		"Project":     "Demo",
	}

	testCases := []struct {
		name        string
		tags        map[string]string
		setupMocks  func(*MockAthenaClient)
		expectLogs  []string
		skipLogTest bool
	}{
		{
			name: "Successfully tag all resources with pagination",
			tags: validTags,
			setupMocks: func(m *MockAthenaClient) {
				// First page of workgroups
				m.On("ListWorkGroups", mock.Anything, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
							{Name: aws.String("primary")}, // Should be skipped
						},
						NextToken: aws.String("next-page"),
					}, nil).Once()

				// Second page of workgroups
				m.On("ListWorkGroups", mock.Anything, &athena.ListWorkGroupsInput{
					NextToken: aws.String("next-page"),
				}).Return(&athena.ListWorkGroupsOutput{
					WorkGroups: []athenatypes.WorkGroupSummary{
						{Name: aws.String("workgroup2")},
					},
				}, nil).Once()

				// First page of data catalogs
				m.On("ListDataCatalogs", mock.Anything, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog1")},
						},
						NextToken: aws.String("next-catalog-page"),
					}, nil).Once()

				// Second page of data catalogs
				m.On("ListDataCatalogs", mock.Anything, &athena.ListDataCatalogsInput{
					NextToken: aws.String("next-catalog-page"),
				}).Return(&athena.ListDataCatalogsOutput{
					DataCatalogsSummary: []athenatypes.DataCatalogSummary{
						{CatalogName: aws.String("custom-catalog2")},
					},
				}, nil).Once()

				// Mock TagResource calls
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					arn := aws.ToString(input.ResourceARN)
					return strings.Contains(arn, "workgroup/workgroup1") ||
						strings.Contains(arn, "workgroup/workgroup2") ||
						strings.Contains(arn, "datacatalog/custom-catalog1") ||
						strings.Contains(arn, "datacatalog/custom-catalog2")
				})).Return(&athena.TagResourceOutput{}, nil).Times(4)
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Successfully tagged Athena workgroup: workgroup1",
				"Successfully tagged Athena workgroup: workgroup2",
				"Successfully tagged Athena data catalog: custom-catalog1",
				"Successfully tagged Athena data catalog: custom-catalog2",
				"Completed tagging Athena resources",
			},
		},
		{
			name: "Handle invalid tags",
			tags: map[string]string{
				"aws:restricted": "value",
			},
			setupMocks: func(m *MockAthenaClient) {
				// No mocks needed as validation should fail first
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Error: Invalid tags configuration: tag key cannot start with 'aws:'",
				"Completed tagging Athena resources",
			},
		},
		{
			name: "Handle ListWorkGroups error",
			tags: validTags,
			setupMocks: func(m *MockAthenaClient) {
				m.On("ListWorkGroups", mock.Anything, mock.Anything).
					Return((*athena.ListWorkGroupsOutput)(nil), fmt.Errorf("API error"))

				// Need to mock ListDataCatalogs as the function continues even after ListWorkGroups error
				m.On("ListDataCatalogs", mock.Anything, mock.Anything).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog")},
						},
					}, nil)

				// Mock TagResource for data catalog
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "datacatalog/custom-catalog")
				})).Return(&athena.TagResourceOutput{}, nil)
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Error tagging Athena workgroups: failed to list workgroups",
				"Successfully tagged Athena data catalog: custom-catalog",
				"Completed tagging Athena resources",
			},
		},
		{
			name: "Handle ListDataCatalogs error",
			tags: validTags,
			setupMocks: func(m *MockAthenaClient) {
				// ListWorkGroups succeeds but returns no workgroups
				m.On("ListWorkGroups", mock.Anything, mock.Anything).
					Return(&athena.ListWorkGroupsOutput{}, nil)

				// ListDataCatalogs fails
				m.On("ListDataCatalogs", mock.Anything, mock.Anything).
					Return((*athena.ListDataCatalogsOutput)(nil), fmt.Errorf("API error"))
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Error tagging Athena data catalogs: failed to list data catalogs",
				"Completed tagging Athena resources",
			},
		},
		{
			name: "Handle TagResource error",
			tags: validTags,
			setupMocks: func(m *MockAthenaClient) {
				// Return one workgroup
				m.On("ListWorkGroups", mock.Anything, mock.Anything).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
						},
					}, nil)

				// Return one catalog
				m.On("ListDataCatalogs", mock.Anything, mock.Anything).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog")},
						},
					}, nil)

				// Fail all TagResource calls
				m.On("TagResource", mock.Anything, mock.Anything).
					Return((*athena.TagResourceOutput)(nil), fmt.Errorf("tagging error"))
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Warning: failed to tag workgroup workgroup1",
				"Warning: failed to tag data catalog custom-catalog",
				"Completed tagging Athena resources",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Capture log output
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			defer log.SetOutput(os.Stderr)

			// Create mock client and set up expectations
			mockClient := new(MockAthenaClient)
			tc.setupMocks(mockClient)

			// Create tagger with test configuration
			tagger := &AWSResourceTagger{
				ctx:       ctx,
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tc.tags,
			}

			// Execute tagging
			tagger.tagAthenaResourcesWithClient(mockClient)

			// Verify mock expectations
			mockClient.AssertExpectations(t)

			// Verify logs if expected
			if !tc.skipLogTest {
				logOutput := logBuffer.String()
				for _, expectedLog := range tc.expectLogs {
					assert.Contains(t, logOutput, expectedLog)
				}
			}
		})
	}
}

func TestTagAthenaResourcesWithEmptyTags(t *testing.T) {
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

	mockClient := new(MockAthenaClient)
	// Since we have the empty tags check, no API calls should be made
	tagger.tagAthenaResourcesWithClient(mockClient)

	// Verify no API calls were made
	mockClient.AssertNotCalled(t, "ListWorkGroups")
	mockClient.AssertNotCalled(t, "ListDataCatalogs")
	mockClient.AssertNotCalled(t, "TagResource")

	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Tagging Athena resources...")
	assert.Contains(t, logOutput, "No tags provided, skipping Athena resource tagging")
	assert.Contains(t, logOutput, "Completed tagging Athena resources")
}

func TestTagAthenaResourcesWithSkipPrimary(t *testing.T) {
	ctx := context.Background()
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

	mockClient := new(MockAthenaClient)

	// Mock ListWorkGroups to return only the primary workgroup
	mockClient.On("ListWorkGroups", mock.Anything, &athena.ListWorkGroupsInput{}).
		Return(&athena.ListWorkGroupsOutput{
			WorkGroups: []athenatypes.WorkGroupSummary{
				{Name: aws.String("primary")}, // Should be skipped
			},
		}, nil)

	// Mock ListDataCatalogs to return empty list
	mockClient.On("ListDataCatalogs", mock.Anything, &athena.ListDataCatalogsInput{}).
		Return(&athena.ListDataCatalogsOutput{
			DataCatalogsSummary: []athenatypes.DataCatalogSummary{},
		}, nil)

	tagger.tagAthenaResourcesWithClient(mockClient)

	// Verify all expectations
	mockClient.AssertExpectations(t)

	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "Tagging Athena resources...")
	assert.Contains(t, logOutput, "Completed tagging Athena resources")
	assert.NotContains(t, logOutput, "Successfully tagged Athena workgroup: primary")
}
