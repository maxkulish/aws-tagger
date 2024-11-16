package tagger

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTagGlueTriggers(t *testing.T) {
	tests := []struct {
		name           string
		triggers       []gluetypes.Trigger
		expectTagging  bool
		setupMockError bool
		expectedFound  int32
		expectedTagged int32
		expectedFailed int32
	}{
		{
			name: "Successfully tag multiple triggers",
			triggers: []gluetypes.Trigger{
				{Name: aws.String("trigger1"), Type: gluetypes.TriggerTypeScheduled},
				{Name: aws.String("trigger2"), Type: gluetypes.TriggerTypeOnDemand},
			},
			expectTagging:  true,
			setupMockError: false,
			expectedFound:  2,
			expectedTagged: 2,
			expectedFailed: 0,
		},
		{
			name:           "Empty trigger list",
			triggers:       []gluetypes.Trigger{},
			expectTagging:  false,
			setupMockError: false,
			expectedFound:  0,
			expectedTagged: 0,
			expectedFailed: 0,
		},
		{
			name: "Tag resource fails for some triggers",
			triggers: []gluetypes.Trigger{
				{Name: aws.String("trigger1"), Type: gluetypes.TriggerTypeScheduled},
				{Name: aws.String("trigger2"), Type: gluetypes.TriggerTypeOnDemand},
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

			// Setup expectations for GetTriggers
			mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
				MaxResults: aws.Int32(100),
			}).Return(&glue.GetTriggersOutput{
				Triggers: tt.triggers,
			}, nil)

			if tt.expectTagging {
				for i, trigger := range tt.triggers {
					expectedArn := tagger.buildCompoundARN(GlueTrigger, aws.ToString(trigger.Name))
					var tagError error
					if tt.setupMockError && i == 1 { // Make the second trigger fail
						tagError = assert.AnError
					}
					mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
						ResourceArn: aws.String(expectedArn),
						TagsToAdd:   tagger.convertToGlueTags(),
					}).Return(&glue.TagResourceOutput{}, tagError)
				}
			}

			// Execute test
			tagger.tagGlueTriggers(mockClient, metrics)

			// Verify expectations
			mockClient.AssertExpectations(t)

			// Verify metrics
			assert.Equal(t, tt.expectedFound, metrics.TriggersFound)
			assert.Equal(t, tt.expectedTagged, metrics.TriggersTagged)
			assert.Equal(t, tt.expectedFailed, metrics.TriggersFailed)
		})
	}
}

func TestTagGlueTriggersGetError(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Setup expectations for GetTriggers to return an error
	mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
		MaxResults: aws.Int32(100),
	}).Return(nil, assert.AnError)

	// Execute test
	tagger.tagGlueTriggers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics reflect the error
	assert.Equal(t, int32(0), metrics.TriggersFound)
	assert.Equal(t, int32(0), metrics.TriggersTagged)
	assert.Equal(t, int32(0), metrics.TriggersFailed)
}

func TestTagGlueTriggersPagination(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Create test data for pagination
	firstPage := []gluetypes.Trigger{
		{Name: aws.String("trigger1"), Type: gluetypes.TriggerTypeScheduled},
		{Name: aws.String("trigger2"), Type: gluetypes.TriggerTypeOnDemand},
	}
	secondPage := []gluetypes.Trigger{
		{Name: aws.String("trigger3"), Type: gluetypes.TriggerTypeScheduled},
		{Name: aws.String("trigger4"), Type: gluetypes.TriggerTypeConditional},
	}

	// Setup expectations for first page
	mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
		MaxResults: aws.Int32(100),
		NextToken:  nil,
	}).Return(&glue.GetTriggersOutput{
		Triggers:  firstPage,
		NextToken: aws.String("next-token"),
	}, nil).Once()

	// Setup expectations for second page
	mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
		MaxResults: aws.Int32(100),
		NextToken:  aws.String("next-token"),
	}).Return(&glue.GetTriggersOutput{
		Triggers:  secondPage,
		NextToken: nil,
	}, nil).Once()

	// Setup TagResource expectations for all triggers
	allTriggers := append(firstPage, secondPage...)
	for _, trigger := range allTriggers {
		expectedArn := tagger.buildCompoundARN(GlueTrigger, aws.ToString(trigger.Name))
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, nil).Once()
	}

	// Execute test
	tagger.tagGlueTriggers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(allTriggers)), metrics.TriggersFound)
	assert.Equal(t, int32(len(allTriggers)), metrics.TriggersTagged)
	assert.Equal(t, int32(0), metrics.TriggersFailed)
}

func TestTagGlueTriggersMixedResults(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Create test data with mixed results across pages
	firstPage := []gluetypes.Trigger{
		{Name: aws.String("trigger1"), Type: gluetypes.TriggerTypeScheduled},
		{Name: aws.String("trigger2"), Type: gluetypes.TriggerTypeOnDemand},
	}
	secondPage := []gluetypes.Trigger{
		{Name: aws.String("trigger3"), Type: gluetypes.TriggerTypeScheduled},
		{Name: aws.String("trigger4"), Type: gluetypes.TriggerTypeConditional},
	}

	// Setup paginated GetTriggers calls
	mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
		MaxResults: aws.Int32(100),
		NextToken:  nil,
	}).Return(&glue.GetTriggersOutput{
		Triggers:  firstPage,
		NextToken: aws.String("next-token"),
	}, nil).Once()

	mockClient.On("GetTriggers", mock.Anything, &glue.GetTriggersInput{
		MaxResults: aws.Int32(100),
		NextToken:  aws.String("next-token"),
	}).Return(&glue.GetTriggersOutput{
		Triggers:  secondPage,
		NextToken: nil,
	}, nil).Once()

	// Setup TagResource with mixed results (success and failures)
	allTriggers := append(firstPage, secondPage...)
	for i, trigger := range allTriggers {
		expectedArn := tagger.buildCompoundARN(GlueTrigger, aws.ToString(trigger.Name))
		var tagError error
		// Make triggers 2 and 3 fail (one from each page)
		if i == 1 || i == 2 {
			tagError = assert.AnError
		}
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, tagError).Once()
	}

	// Execute test
	tagger.tagGlueTriggers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(allTriggers)), metrics.TriggersFound)
	assert.Equal(t, int32(2), metrics.TriggersTagged) // 2 successful tags
	assert.Equal(t, int32(2), metrics.TriggersFailed) // 2 failed tags
}
