package tagger

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTagGlueCrawlers(t *testing.T) {
	tests := []struct {
		name           string
		crawlers       []gluetypes.Crawler
		expectTagging  bool
		setupMockError bool
		expectedFound  int32
		expectedTagged int32
		expectedFailed int32
	}{
		{
			name: "Successfully tag multiple crawlers",
			crawlers: []gluetypes.Crawler{
				{Name: aws.String("crawler1")},
				{Name: aws.String("crawler2")},
			},
			expectTagging:  true,
			setupMockError: false,
			expectedFound:  2,
			expectedTagged: 2,
			expectedFailed: 0,
		},
		{
			name:           "Empty crawler list",
			crawlers:       []gluetypes.Crawler{},
			expectTagging:  false,
			setupMockError: false,
			expectedFound:  0,
			expectedTagged: 0,
			expectedFailed: 0,
		},
		{
			name: "Tag resource fails for some crawlers",
			crawlers: []gluetypes.Crawler{
				{Name: aws.String("crawler1")},
				{Name: aws.String("crawler2")},
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

			// Setup expectations for GetCrawlers
			mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
				MaxResults: aws.Int32(100),
			}).Return(&glue.GetCrawlersOutput{
				Crawlers: tt.crawlers,
			}, nil)

			if tt.expectTagging {
				for i, crawler := range tt.crawlers {
					expectedArn := tagger.buildCompoundARN(GlueCrawler, aws.ToString(crawler.Name))
					var tagError error
					if tt.setupMockError && i == 1 { // Make the second crawler fail
						tagError = assert.AnError
					}
					mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
						ResourceArn: aws.String(expectedArn),
						TagsToAdd:   tagger.convertToGlueTags(),
					}).Return(&glue.TagResourceOutput{}, tagError)
				}
			}

			// Execute test
			tagger.tagGlueCrawlers(mockClient, metrics)

			// Verify expectations
			mockClient.AssertExpectations(t)

			// Verify metrics
			assert.Equal(t, tt.expectedFound, metrics.CrawlersFound)
			assert.Equal(t, tt.expectedTagged, metrics.CrawlersTagged)
			assert.Equal(t, tt.expectedFailed, metrics.CrawlersFailed)
		})
	}
}

func TestTagGlueCrawlersGetError(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Setup expectations for GetCrawlers to return an error
	mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
		MaxResults: aws.Int32(100),
	}).Return(nil, assert.AnError)

	// Execute test
	tagger.tagGlueCrawlers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics reflect the error
	assert.Equal(t, int32(0), metrics.CrawlersFound)
	assert.Equal(t, int32(0), metrics.CrawlersTagged)
	assert.Equal(t, int32(0), metrics.CrawlersFailed)
}

func TestTagGlueCrawlersPagination(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Create test data for pagination
	firstPage := []gluetypes.Crawler{
		{Name: aws.String("crawler1")},
		{Name: aws.String("crawler2")},
	}
	secondPage := []gluetypes.Crawler{
		{Name: aws.String("crawler3")},
		{Name: aws.String("crawler4")},
	}

	// Setup expectations for first page
	mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
		MaxResults: aws.Int32(100),
		NextToken:  nil,
	}).Return(&glue.GetCrawlersOutput{
		Crawlers:  firstPage,
		NextToken: aws.String("next-token"),
	}, nil).Once()

	// Setup expectations for second page
	mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
		MaxResults: aws.Int32(100),
		NextToken:  aws.String("next-token"),
	}).Return(&glue.GetCrawlersOutput{
		Crawlers:  secondPage,
		NextToken: nil,
	}, nil).Once()

	// Setup TagResource expectations for all crawlers
	allCrawlers := append(firstPage, secondPage...)
	for _, crawler := range allCrawlers {
		expectedArn := tagger.buildCompoundARN(GlueCrawler, aws.ToString(crawler.Name))
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, nil).Once()
	}

	// Execute test
	tagger.tagGlueCrawlers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(allCrawlers)), metrics.CrawlersFound)
	assert.Equal(t, int32(len(allCrawlers)), metrics.CrawlersTagged)
	assert.Equal(t, int32(0), metrics.CrawlersFailed)
}

func TestTagGlueCrawlersMixedResults(t *testing.T) {
	// Create mock client
	mockClient := new(MockGlueClient)
	tagger := createTestTagger()
	metrics := &GlueMetrics{}

	// Create test data with mixed results across pages
	firstPage := []gluetypes.Crawler{
		{Name: aws.String("crawler1"), State: gluetypes.CrawlerStateReady},
		{Name: aws.String("crawler2"), State: gluetypes.CrawlerStateRunning},
	}
	secondPage := []gluetypes.Crawler{
		{Name: aws.String("crawler3"), State: gluetypes.CrawlerStateStopping},
		{Name: aws.String("crawler4"), State: gluetypes.CrawlerStateReady},
	}

	// Setup paginated GetCrawlers calls
	mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
		MaxResults: aws.Int32(100),
		NextToken:  nil,
	}).Return(&glue.GetCrawlersOutput{
		Crawlers:  firstPage,
		NextToken: aws.String("next-token"),
	}, nil).Once()

	mockClient.On("GetCrawlers", mock.Anything, &glue.GetCrawlersInput{
		MaxResults: aws.Int32(100),
		NextToken:  aws.String("next-token"),
	}).Return(&glue.GetCrawlersOutput{
		Crawlers:  secondPage,
		NextToken: nil,
	}, nil).Once()

	// Setup TagResource with mixed results (success and failures)
	allCrawlers := append(firstPage, secondPage...)
	for i, crawler := range allCrawlers {
		expectedArn := tagger.buildCompoundARN(GlueCrawler, aws.ToString(crawler.Name))
		var tagError error
		// Make crawlers 2 and 3 fail (one from each page)
		if i == 1 || i == 2 {
			tagError = assert.AnError
		}
		mockClient.On("TagResource", mock.Anything, &glue.TagResourceInput{
			ResourceArn: aws.String(expectedArn),
			TagsToAdd:   tagger.convertToGlueTags(),
		}).Return(&glue.TagResourceOutput{}, tagError).Once()
	}

	// Execute test
	tagger.tagGlueCrawlers(mockClient, metrics)

	// Verify expectations
	mockClient.AssertExpectations(t)

	// Verify metrics
	assert.Equal(t, int32(len(allCrawlers)), metrics.CrawlersFound)
	assert.Equal(t, int32(2), metrics.CrawlersTagged) // 2 successful tags
	assert.Equal(t, int32(2), metrics.CrawlersFailed) // 2 failed tags
}
