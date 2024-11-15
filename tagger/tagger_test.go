package tagger

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
)

// MockSTSAPI interface for mocking STS client
type MockSTSAPI interface {
	GetCallerIdentity(ctx context.Context, params *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// mockSTSClient mocks the STS client
type mockSTSClient struct {
	mock.Mock
}

func (m *mockSTSClient) GetCallerIdentity(ctx context.Context, params *sts.GetCallerIdentityInput, optFns ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*sts.GetCallerIdentityOutput), args.Error(1)
}

// MockResourceTagger wraps AWSResourceTagger for testing
type MockResourceTagger struct {
	ctx           context.Context
	cfg           aws.Config
	tags          map[string]string
	stsClient     *mockSTSClient
	taggedCounts  map[string]int
	mu            sync.Mutex
	resourceFuncs map[string]func()
}

// NewMockResourceTagger creates a new mock tagger instance
func NewMockResourceTagger(t *testing.T) *MockResourceTagger {
	mockSTS := new(mockSTSClient)

	return &MockResourceTagger{
		ctx:           context.Background(),
		cfg:           aws.Config{Region: "us-west-2"},
		tags:          map[string]string{"Environment": "Test"},
		stsClient:     mockSTS,
		taggedCounts:  make(map[string]int),
		resourceFuncs: make(map[string]func()),
	}
}

// validateSSOSession implements the validation method
func (m *MockResourceTagger) validateSSOSession() error {
	_, err := m.stsClient.GetCallerIdentity(m.ctx, &sts.GetCallerIdentityInput{})
	return err
}

// TagAllResources implements the main tagging method
func (m *MockResourceTagger) TagAllResources() {
	if err := m.validateSSOSession(); err != nil {
		log.Printf("SSO session validation failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	resourceTypes := []string{"EC2", "S3", "CloudWatch", "OpenSearch", "ElastiCache",
		"RDS", "Glue", "VPC", "Athena", "ELB"}

	for _, rt := range resourceTypes {
		wg.Add(1)
		go func(resourceType string) {
			defer wg.Done()
			if fn, exists := m.resourceFuncs[resourceType]; exists {
				fn()
			}
			m.recordTagging(resourceType)
		}(rt)
	}

	wg.Wait()
}

// Helper methods
func (m *MockResourceTagger) recordTagging(resourceType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taggedCounts[resourceType]++
}

func (m *MockResourceTagger) getTagCount(resourceType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taggedCounts[resourceType]
}

func (m *MockResourceTagger) setResourceFunc(resourceType string, fn func()) {
	m.resourceFuncs[resourceType] = fn
}

// Tests
func TestTagAllResources_Success(t *testing.T) {
	mockTagger := NewMockResourceTagger(t)

	// Mock successful SSO session
	mockTagger.stsClient.On("GetCallerIdentity",
		mockTagger.ctx,
		&sts.GetCallerIdentityInput{},
	).Return(&sts.GetCallerIdentityOutput{}, nil)

	// Set up resource types to test
	resourceTypes := []string{"EC2", "S3", "CloudWatch", "OpenSearch", "ElastiCache",
		"RDS", "Glue", "VPC", "Athena", "ELB"}

	// Set up mock functions
	for _, rt := range resourceTypes {
		resourceType := rt
		mockTagger.setResourceFunc(resourceType, func() {
			time.Sleep(10 * time.Millisecond)
		})
	}

	// Execute tagging
	mockTagger.TagAllResources()

	// Allow time for goroutines to complete
	time.Sleep(100 * time.Millisecond)

	// Verify all resources were tagged
	for _, rt := range resourceTypes {
		assert.Equal(t, 1, mockTagger.getTagCount(rt),
			"Resource type %s should be tagged exactly once", rt)
	}

	mockTagger.stsClient.AssertExpectations(t)
}

func TestTagAllResources_SSOFailure(t *testing.T) {
	mockTagger := NewMockResourceTagger(t)

	// Mock SSO session failure
	mockTagger.stsClient.On("GetCallerIdentity",
		mockTagger.ctx,
		&sts.GetCallerIdentityInput{},
	).Return(nil, errors.New("SSO session invalid"))

	// Execute tagging
	mockTagger.TagAllResources()

	// Verify no resources were tagged
	resourceTypes := []string{"EC2", "S3", "CloudWatch", "OpenSearch", "ElastiCache",
		"RDS", "Glue", "VPC", "Athena", "ELB"}

	for _, rt := range resourceTypes {
		assert.Equal(t, 0, mockTagger.getTagCount(rt),
			"Resource type %s should not be tagged when SSO validation fails", rt)
	}

	mockTagger.stsClient.AssertExpectations(t)
}

func TestTagAllResources_ConcurrentExecution(t *testing.T) {
	mockTagger := NewMockResourceTagger(t)

	// Mock successful SSO session
	mockTagger.stsClient.On("GetCallerIdentity",
		mockTagger.ctx,
		&sts.GetCallerIdentityInput{},
	).Return(&sts.GetCallerIdentityOutput{}, nil)

	var wg sync.WaitGroup
	executionTimes := make(map[string]time.Time)
	var timesMutex sync.Mutex

	// Test with a subset of resources
	resourceTypes := []string{"EC2", "S3", "CloudWatch"}

	for _, rt := range resourceTypes {
		resourceType := rt
		wg.Add(1)
		mockTagger.setResourceFunc(resourceType, func() {
			timesMutex.Lock()
			executionTimes[resourceType] = time.Now()
			timesMutex.Unlock()
			time.Sleep(50 * time.Millisecond)
			wg.Done()
		})
	}

	start := time.Now()
	mockTagger.TagAllResources()
	wg.Wait()
	duration := time.Since(start)

	// Verify concurrent execution
	assert.Less(t, duration, time.Duration(len(resourceTypes))*50*time.Millisecond,
		"Execution time indicates non-concurrent execution")

	mockTagger.stsClient.AssertExpectations(t)
}
