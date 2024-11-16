package tagger

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTagGlueJobs(t *testing.T) {
	tests := []struct {
		name           string
		jobs           []gluetypes.Job
		expectTagging  bool
		setupMockError bool
		expectedFound  int32
		expectedTagged int32
		expectedFailed int32
	}{
		{
			name: "Successfully tag multiple jobs",
			jobs: []gluetypes.Job{
				{Name: aws.String("job1")},
				{Name: aws.String("job2")},
			},
			expectTagging:  true,
			setupMockError: false,
			expectedFound:  2,
			expectedTagged: 2,
			expectedFailed: 0,
		},
		{
			name:           "Empty job list",
			jobs:           []gluetypes.Job{},
			expectTagging:  false,
			setupMockError: false,
			expectedFound:  0,
			expectedTagged: 0,
			expectedFailed: 0,
		},
		{
			name: "Tag resource fails for some jobs",
			jobs: []gluetypes.Job{
				{Name: aws.String("job1")},
				{Name: aws.String("job2")},
			},
			expectTagging:  true,
			setupMockError: true,
			expectedFound:  2,
			expectedTagged: 1,
			expectedFailed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client
			mockClient := new(MockGlueClient)
			tagger := createTestTagger()
			metrics := &GlueMetrics{}

			// Setup expectations for GetJobs
			mockClient.On("GetJobs", mock.Anything, &glue.GetJobsInput{
				MaxResults: aws.Int32(100),
			}).Return(&glue.GetJobsOutput{
				Jobs: tt.jobs,
			}, nil)

			if tt.expectTagging {
				for i, job := range tt.jobs {
					expectedArn := tagger.buildCompoundARN(GlueJob, aws.ToString(job.Name))
					var tagError error
					if tt.setupMockError && i == 1 { // Make the second job fail
						tagError = assert.AnError
					}
					mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
						ResourceArn: aws.String(expectedArn),
						TagsToAdd:   tagger.convertToGlueTags(),
					}).Return(&glue.TagResourceOutput{}, tagError)
				}
			}

			// Execute test
			tagger.tagGlueJobs(mockClient, metrics)

			// Verify expectations
			mockClient.AssertExpectations(t)

			// Verify metrics
			assert.Equal(t, tt.expectedFound, metrics.JobsFound)
			assert.Equal(t, tt.expectedTagged, metrics.JobsTagged)
			assert.Equal(t, tt.expectedFailed, metrics.JobsFailed)
		})
	}
}

func TestTagGlueJobsGetError(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Setup expectations for GetJobs to return an error
	mockClient.On("GetJobs", mock.Anything, &glue.GetJobsInput{
		MaxResults: aws.Int32(100),
	}).Return(nil, assert.AnError)

	// Execute test
	tagger.tagGlueJobs(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics reflect the error
	assert.Equal(t, int32(0), metrics.JobsFound)
	assert.Equal(t, int32(0), metrics.JobsTagged)
	assert.Equal(t, int32(0), metrics.JobsFailed)
}

func TestTagGlueJobsPagination(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Create test data for pagination
	firstPage := []gluetypes.Job{
		{Name: aws.String("job1")},
		{Name: aws.String("job2")},
	}
	secondPage := []gluetypes.Job{
		{Name: aws.String("job3")},
		{Name: aws.String("job4")},
	}

	// Setup expectations for first page
	mockClient.On("GetJobs", mock.Anything, &glue.GetJobsInput{
		MaxResults: aws.Int32(100),
		NextToken:  nil,
	}).Return(&glue.GetJobsOutput{
		Jobs:      firstPage,
		NextToken: aws.String("next-token"),
	}, nil).Once()

	// Setup expectations for second page
	mockClient.On("GetJobs", mock.Anything, &glue.GetJobsInput{
		MaxResults: aws.Int32(100),
		NextToken:  aws.String("next-token"),
	}).Return(&glue.GetJobsOutput{
		Jobs:      secondPage,
		NextToken: nil,
	}, nil).Once()

	// Setup TagResource expectations for all jobs
	allJobs := append(firstPage, secondPage...)
	for _, job := range allJobs {
		expectedArn := tagger.buildCompoundARN(GlueJob, aws.ToString(job.Name))
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, nil).Once()
	}

	// Execute test
	tagger.tagGlueJobs(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(allJobs)), metrics.JobsFound)
	assert.Equal(t, int32(len(allJobs)), metrics.JobsTagged)
	assert.Equal(t, int32(0), metrics.JobsFailed)
}
