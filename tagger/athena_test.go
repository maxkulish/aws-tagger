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

func _TestTagAthenaWorkgroups(t *testing.T) {
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

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:workgroup/workgroup1")
				})).Return(&athena.TagResourceOutput{}, nil)

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:workgroup/workgroup2")
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

				m.On("TagResource", ctx, mock.Anything).
					Return(&athena.TagResourceOutput{}, fmt.Errorf("tagging error"))
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

func _TestTagAthenaDataCatalogs(t *testing.T) {
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

				m.On("TagResource", ctx, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return aws.ToString(input.ResourceARN) == fmt.Sprintf("arn:aws:athena:us-west-2:123456789012:datacatalog/custom-catalog")
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

func _TestTagAthenaResources(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name        string
		tags        map[string]string
		setupMocks  func(*MockAthenaClient)
		expectLogs  []string
		skipLogTest bool
	}{
		{
			name: "Successfully tag all resources",
			tags: map[string]string{
				"Environment": "Test",
				"Project":     "Demo",
			},
			setupMocks: func(m *MockAthenaClient) {
				// Mock ListWorkGroups
				m.On("ListWorkGroups", mock.Anything, &athena.ListWorkGroupsInput{}).
					Return(&athena.ListWorkGroupsOutput{
						WorkGroups: []athenatypes.WorkGroupSummary{
							{Name: aws.String("workgroup1")},
							{Name: aws.String("primary")}, // Should be skipped
						},
					}, nil)

				// Mock ListDataCatalogs
				m.On("ListDataCatalogs", mock.Anything, &athena.ListDataCatalogsInput{}).
					Return(&athena.ListDataCatalogsOutput{
						DataCatalogsSummary: []athenatypes.DataCatalogSummary{
							{CatalogName: aws.String("custom-catalog")},
							{CatalogName: aws.String("AwsDataCatalog")}, // Should be skipped
						},
					}, nil)

				// Mock TagResource for workgroup
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "workgroup/workgroup1")
				})).Return(&athena.TagResourceOutput{}, nil)

				// Mock TagResource for data catalog
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *athena.TagResourceInput) bool {
					return strings.Contains(aws.ToString(input.ResourceARN), "datacatalog/custom-catalog")
				})).Return(&athena.TagResourceOutput{}, nil)
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Successfully tagged Athena workgroup: workgroup1",
				"Successfully tagged Athena data catalog: custom-catalog",
				"Completed tagging Athena resources",
			},
		},
		{
			name: "Handle invalid tags",
			tags: map[string]string{
				"aws:restricted": "value", // Invalid tag with aws: prefix
			},
			setupMocks: func(m *MockAthenaClient) {
				// No mocks needed as validation should fail first
			},
			expectLogs: []string{
				"Tagging Athena resources...",
				"Error: Invalid tags configuration: tag key cannot start with 'aws:': aws:restricted",
				"Completed tagging Athena resources",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a buffer to capture log output
			var logBuffer bytes.Buffer
			log.SetOutput(&logBuffer)
			// Restore logger output after test
			defer log.SetOutput(os.Stderr)

			// Create mock client and set up expectations
			mockClient := new(MockAthenaClient)
			tc.setupMocks(mockClient)

			// Create tagger with test configuration
			tagger := &AWSResourceTagger{
				ctx:       ctx,
				cfg:       aws.Config{Region: "us-west-2"},
				accountID: "123456789012",
				tags:      tc.tags,
			}

			// Execute tagging with mock client - Use tagAthenaResourcesWithClient instead of tagAthenaResources
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
