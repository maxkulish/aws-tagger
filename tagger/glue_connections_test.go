package tagger

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTagGlueConnectionsBasic(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Test data
	connections := []gluetypes.Connection{
		{Name: aws.String("connection1")},
		{Name: aws.String("connection2")},
	}

	// Setup GetConnections expectation
	mockClient.On("GetConnections", mock.Anything, &glue.GetConnectionsInput{}).
		Return(&glue.GetConnectionsOutput{
			ConnectionList: connections,
		}, nil)

	// Setup TagResource expectations
	for _, conn := range connections {
		expectedArn := tagger.buildCompoundARN(GlueConnection, aws.ToString(conn.Name))
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, nil)
	}

	// Execute test
	tagger.tagGlueConnections(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(connections)), metrics.ConnectionsFound)
	assert.Equal(t, int32(len(connections)), metrics.ConnectionsTagged)
	assert.Equal(t, int32(0), metrics.ConnectionsFailed)
}
