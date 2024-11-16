// arn_test.go
package tagger

import (
	"testing"
)

func TestBuildARN(t *testing.T) {
	tagger := &AWSResourceTagger{
		region:    "us-west-2",
		accountID: "123456789012",
	}

	tests := []struct {
		name         string
		resourceType ResourceType
		resourceName string
		expected     string
	}{
		{
			name:         "Athena Workgroup",
			resourceType: AthenaWorkgroup,
			resourceName: "primary",
			expected:     "arn:aws:athena:us-west-2:123456789012:workgroup/primary",
		},
		{
			name:         "Athena Catalog",
			resourceType: AthenaCatalog,
			resourceName: "AwsDataCatalog",
			expected:     "arn:aws:athena:us-west-2:123456789012:datacatalog/AwsDataCatalog",
		},
		{
			name:         "Glue Database",
			resourceType: GlueDatabase,
			resourceName: "mydb",
			expected:     "arn:aws:glue:us-west-2:123456789012:database/mydb",
		},
		{
			name:         "Glue Database with trailing slash",
			resourceType: GlueDatabase,
			resourceName: "mydb/",
			expected:     "arn:aws:glue:us-west-2:123456789012:database/mydb",
		},
		{
			name:         "Glue Database with leading slash",
			resourceType: GlueDatabase,
			resourceName: "/mydb",
			expected:     "arn:aws:glue:us-west-2:123456789012:database/mydb",
		},
		{
			name:         "Glue Table",
			resourceType: GlueTable,
			resourceName: "mydb/mytable",
			expected:     "arn:aws:glue:us-west-2:123456789012:table/mydb/mytable",
		},
		{
			name:         "Glue Job",
			resourceType: GlueJob,
			resourceName: "etl-job",
			expected:     "arn:aws:glue:us-west-2:123456789012:job/etl-job",
		},
		{
			name:         "Glue Crawler",
			resourceType: GlueCrawler,
			resourceName: "my-crawler",
			expected:     "arn:aws:glue:us-west-2:123456789012:crawler/my-crawler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tagger.buildARN(tt.resourceType, tt.resourceName)
			if got != tt.expected {
				t.Errorf("buildARN() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBuildCompoundARN(t *testing.T) {
	tagger := &AWSResourceTagger{
		region:    "us-west-2",
		accountID: "123456789012",
	}

	tests := []struct {
		name         string
		resourceType ResourceType
		parts        []string
		expected     string
	}{
		{
			name:         "Glue Table",
			resourceType: GlueTable,
			parts:        []string{"mydb", "mytable"},
			expected:     "arn:aws:glue:us-west-2:123456789012:table/mydb/mytable",
		},
		{
			name:         "Glue Table with slashes",
			resourceType: GlueTable,
			parts:        []string{"/mydb/", "/mytable/"},
			expected:     "arn:aws:glue:us-west-2:123456789012:table/mydb/mytable",
		},
		{
			name:         "Empty parts",
			resourceType: GlueTable,
			parts:        []string{"", "mytable"},
			expected:     "arn:aws:glue:us-west-2:123456789012:table/mytable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tagger.buildCompoundARN(tt.resourceType, tt.parts...)
			if got != tt.expected {
				t.Errorf("buildCompoundARN() = %v, want %v", got, tt.expected)
			}
		})
	}
}
