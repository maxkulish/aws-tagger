package tagger

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockGlueClient is a mock implementation of GlueAPI
type MockGlueClient struct {
	mock.Mock
}

// GetDatabases mock implementation
func (m *MockGlueClient) GetDatabases(ctx context.Context, params *glue.GetDatabasesInput, optFns ...func(*glue.Options)) (*glue.GetDatabasesOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetDatabasesOutput), args.Error(1)
}

// TagResource mock implementation
func (m *MockGlueClient) TagResource(ctx context.Context, params *glue.TagResourceInput, optFns ...func(*glue.Options)) (*glue.TagResourceOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.TagResourceOutput), args.Error(1)
}

// GetConnections mock implementation
func (m *MockGlueClient) GetConnections(ctx context.Context, params *glue.GetConnectionsInput, optFns ...func(*glue.Options)) (*glue.GetConnectionsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetConnectionsOutput), args.Error(1)
}

// GetCrawlers mock implementation
func (m *MockGlueClient) GetCrawlers(ctx context.Context, params *glue.GetCrawlersInput, optFns ...func(*glue.Options)) (*glue.GetCrawlersOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetCrawlersOutput), args.Error(1)
}

// GetJobs mock implementation
func (m *MockGlueClient) GetJobs(ctx context.Context, params *glue.GetJobsInput, optFns ...func(*glue.Options)) (*glue.GetJobsOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetJobsOutput), args.Error(1)
}

// GetTriggersmock implementation
func (m *MockGlueClient) GetTriggers(ctx context.Context, params *glue.GetTriggersInput, optFns ...func(*glue.Options)) (*glue.GetTriggersOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*glue.GetTriggersOutput), args.Error(1)
}

// Helper function to create a test tagger instance
func createTestTagger() *AWSResourceTagger {
	return &AWSResourceTagger{
		ctx:       context.Background(),
		accountID: "123456789012",
		region:    "us-west-2",
		tags: map[string]string{
			"Environment": "Test",
			"Project":     "UnitTest",
		},
	}
}

func TestTagGlueDatabases(t *testing.T) {
	tests := []struct {
		name          string
		databases     []gluetypes.Database
		expectTagging bool
		expectError   bool
	}{
		{
			name: "Successfully tag multiple databases",
			databases: []gluetypes.Database{
				{Name: aws.String("database1")},
				{Name: aws.String("database2")},
			},
			expectTagging: true,
			expectError:   false,
		},
		{
			name:          "Empty database list",
			databases:     []gluetypes.Database{},
			expectTagging: false,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockGlueClient)
			tagger := createTestTagger()
			metrics := &GlueMetrics{}

			// Setup expectations
			mockClient.On("GetDatabases", mock.Anything, mock.Anything).
				Return(&glue.GetDatabasesOutput{
					DatabaseList: tt.databases,
				}, nil)

			if tt.expectTagging {
				for _, db := range tt.databases {
					expectedArn := tagger.buildCompoundARN(GlueDatabase, aws.ToString(db.Name))
					mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
						ResourceArn: aws.String(expectedArn),
						TagsToAdd:   tagger.convertToGlueTags(),
					}).Return(&glue.TagResourceOutput{}, nil)
				}
			}

			// Execute test
			tagger.tagGlueDatabases(mockClient, metrics)

			// Verify expectations
			mockClient.AssertExpectations(t)

			// Verify metrics
			assert.Equal(t, int32(len(tt.databases)), metrics.DatabasesFound)
			if tt.expectTagging {
				assert.Equal(t, int32(len(tt.databases)), metrics.DatabasesTagged)
				assert.Equal(t, int32(0), metrics.DatabasesFailed)
			}
		})
	}
}

func TestTagGlueDatabasesError(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Setup expectations for GetDatabases to return an error
	mockClient.On("GetDatabases", mock.Anything, mock.Anything).
		Return(nil, assert.AnError)

	// Execute test
	tagger.tagGlueDatabases(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics reflect the error
	assert.Equal(t, int32(0), metrics.DatabasesFound)
	assert.Equal(t, int32(0), metrics.DatabasesTagged)
	assert.Equal(t, int32(0), metrics.DatabasesFailed)
}

func TestTagGlueResourcesWithClient(t *testing.T) {
	tests := []struct {
		name                  string
		setupMock             func(*MockGlueClient)
		invalidTags           bool
		expectedDatabases     int32
		expectedConnections   int32
		expectedJobs          int32
		expectedCrawlers      int32
		expectedTriggers      int32
		expectedFailedMetrics bool
	}{
		{
			name: "Successfully tag all resources",
			setupMock: func(m *MockGlueClient) {
				// Mock successful database calls
				m.On("GetDatabases", mock.Anything, mock.Anything).
					Return(&glue.GetDatabasesOutput{
						DatabaseList: []gluetypes.Database{
							{Name: aws.String("db1")},
						},
					}, nil)
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *glue.TagResourceInput) bool {
					return aws.ToString(input.ResourceArn) == "arn:aws:glue:us-west-2:123456789012:database/db1"
				})).Return(&glue.TagResourceOutput{}, nil)

				// Mock successful connection calls
				m.On("GetConnections", mock.Anything, mock.Anything).
					Return(&glue.GetConnectionsOutput{
						ConnectionList: []gluetypes.Connection{
							{Name: aws.String("conn1")},
						},
					}, nil)
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *glue.TagResourceInput) bool {
					return aws.ToString(input.ResourceArn) == "arn:aws:glue:us-west-2:123456789012:connection/conn1"
				})).Return(&glue.TagResourceOutput{}, nil)

				// Mock successful jobs calls
				m.On("GetJobs", mock.Anything, mock.Anything).
					Return(&glue.GetJobsOutput{
						Jobs: []gluetypes.Job{
							{Name: aws.String("job1")},
						},
					}, nil)
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *glue.TagResourceInput) bool {
					return aws.ToString(input.ResourceArn) == "arn:aws:glue:us-west-2:123456789012:job/job1"
				})).Return(&glue.TagResourceOutput{}, nil)

				// Mock successful crawlers calls
				m.On("GetCrawlers", mock.Anything, mock.Anything).
					Return(&glue.GetCrawlersOutput{
						Crawlers: []gluetypes.Crawler{
							{Name: aws.String("crawler1")},
						},
					}, nil)
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *glue.TagResourceInput) bool {
					return aws.ToString(input.ResourceArn) == "arn:aws:glue:us-west-2:123456789012:crawler/crawler1"
				})).Return(&glue.TagResourceOutput{}, nil)

				// Mock successful triggers calls
				m.On("GetTriggers", mock.Anything, mock.Anything).
					Return(&glue.GetTriggersOutput{
						Triggers: []gluetypes.Trigger{
							{Name: aws.String("trigger1")},
						},
					}, nil)
				m.On("TagResource", mock.Anything, mock.MatchedBy(func(input *glue.TagResourceInput) bool {
					return aws.ToString(input.ResourceArn) == "arn:aws:glue:us-west-2:123456789012:trigger/trigger1"
				})).Return(&glue.TagResourceOutput{}, nil)
			},
			invalidTags:           false,
			expectedDatabases:     1,
			expectedConnections:   1,
			expectedJobs:          1,
			expectedCrawlers:      1,
			expectedTriggers:      1,
			expectedFailedMetrics: false,
		},
		{
			name: "Invalid tags configuration",
			setupMock: func(m *MockGlueClient) {
				// No mocks needed as it should return early
			},
			invalidTags:           true,
			expectedDatabases:     0,
			expectedConnections:   0,
			expectedJobs:          0,
			expectedCrawlers:      0,
			expectedTriggers:      0,
			expectedFailedMetrics: false,
		},
		{
			name: "Resource API failures",
			setupMock: func(m *MockGlueClient) {
				// Mock failed API calls
				m.On("GetDatabases", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
				m.On("GetConnections", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
				m.On("GetJobs", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
				m.On("GetCrawlers", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
				m.On("GetTriggers", mock.Anything, mock.Anything).
					Return(nil, errors.New("API error"))
			},
			invalidTags:           false,
			expectedDatabases:     0,
			expectedConnections:   0,
			expectedJobs:          0,
			expectedCrawlers:      0,
			expectedTriggers:      0,
			expectedFailedMetrics: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockGlueClient)
			tagger := createTestTagger() // This now includes context

			// Setup invalid tags if needed
			if tt.invalidTags {
				tagger.tags = map[string]string{
					"": "invalid",
				}
			}

			// Setup mock expectations
			tt.setupMock(mockClient)

			// Execute test
			tagger.tagGlueResourcesWithClient(mockClient)

			// Verify expectations
			mockClient.AssertExpectations(t)
		})
	}
}

func TestTagGlueResources(t *testing.T) {
	tests := []struct {
		name      string
		setupTags map[string]string
		wantErr   bool
	}{
		{
			name: "Valid configuration",
			setupTags: map[string]string{
				"Environment": "test",
				"Project":     "unittest",
			},
			wantErr: false,
		},
		{
			name: "Invalid tags",
			setupTags: map[string]string{
				"": "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test AWS configuration
			cfg := aws.Config{}

			// Create a tagger with test configuration including context
			tagger := &AWSResourceTagger{
				ctx:       context.Background(),
				cfg:       cfg,
				accountID: "123456789012",
				region:    "us-west-2",
				tags:      tt.setupTags,
			}

			// Execute tagGlueResources
			tagger.tagGlueResources()
		})
	}
}

// Helper function to verify resource metrics
func verifyResourceMetrics(t *testing.T, metrics *GlueMetrics, expected *GlueMetrics) {
	assert.Equal(t, expected.DatabasesFound, metrics.DatabasesFound, "Databases found mismatch")
	assert.Equal(t, expected.DatabasesTagged, metrics.DatabasesTagged, "Databases tagged mismatch")
	assert.Equal(t, expected.DatabasesFailed, metrics.DatabasesFailed, "Databases failed mismatch")

	assert.Equal(t, expected.ConnectionsFound, metrics.ConnectionsFound, "Connections found mismatch")
	assert.Equal(t, expected.ConnectionsTagged, metrics.ConnectionsTagged, "Connections tagged mismatch")
	assert.Equal(t, expected.ConnectionsFailed, metrics.ConnectionsFailed, "Connections failed mismatch")

	assert.Equal(t, expected.JobsFound, metrics.JobsFound, "Jobs found mismatch")
	assert.Equal(t, expected.JobsTagged, metrics.JobsTagged, "Jobs tagged mismatch")
	assert.Equal(t, expected.JobsFailed, metrics.JobsFailed, "Jobs failed mismatch")

	assert.Equal(t, expected.CrawlersFound, metrics.CrawlersFound, "Crawlers found mismatch")
	assert.Equal(t, expected.CrawlersTagged, metrics.CrawlersTagged, "Crawlers tagged mismatch")
	assert.Equal(t, expected.CrawlersFailed, metrics.CrawlersFailed, "Crawlers failed mismatch")

	assert.Equal(t, expected.TriggersFound, metrics.TriggersFound, "Triggers found mismatch")
	assert.Equal(t, expected.TriggersTagged, metrics.TriggersTagged, "Triggers tagged mismatch")
	assert.Equal(t, expected.TriggersFailed, metrics.TriggersFailed, "Triggers failed mismatch")
}
