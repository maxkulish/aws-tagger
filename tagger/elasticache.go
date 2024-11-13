package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	elctypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
)

// tagElastiCacheResources tags ElastiCache clusters and replication groups
func (t *AWSResourceTagger) tagElastiCacheResources() {
	fmt.Println("=====================================")
	log.Println("Tagging ElastiCache resources...")
	client := elasticache.NewFromConfig(t.cfg)

	// List all ElastiCache clusters
	clusters, err := client.DescribeCacheClusters(t.ctx, &elasticache.DescribeCacheClustersInput{})
	if err != nil {
		t.handleError(err, "all", "ElastiCache")
		return
	}

	// Tag individual clusters
	for _, cluster := range clusters.CacheClusters {
		arn := aws.ToString(cluster.ARN)
		input := &elasticache.AddTagsToResourceInput{
			ResourceName: cluster.ARN,
			Tags: func() []elctypes.Tag {
				tags := make([]elctypes.Tag, 0, len(t.tags))
				for k, v := range t.tags {
					tags = append(tags, elctypes.Tag{
						Key:   aws.String(k),
						Value: aws.String(v),
					})
				}
				return tags
			}(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "ElastiCache")
			continue
		}
		log.Printf("Successfully tagged ElastiCache cluster: %s", aws.ToString(cluster.CacheClusterId))
	}

	// List all Replication Groups
	repGroups, err := client.DescribeReplicationGroups(t.ctx, &elasticache.DescribeReplicationGroupsInput{})
	if err != nil {
		t.handleError(err, "all", "ElastiCache Replication Groups")
		return
	}

	// Tag replication groups
	for _, group := range repGroups.ReplicationGroups {
		arn := aws.ToString(group.ARN)
		input := &elasticache.AddTagsToResourceInput{
			ResourceName: group.ARN,
			Tags: func() []elctypes.Tag {
				tags := make([]elctypes.Tag, 0, len(t.tags))
				for k, v := range t.tags {
					tags = append(tags, elctypes.Tag{
						Key:   aws.String(k),
						Value: aws.String(v),
					})
				}
				return tags
			}(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "ElastiCache Replication Group")
			continue
		}
		log.Printf("Successfully tagged ElastiCache replication group: %s", aws.ToString(group.ReplicationGroupId))
	}

	log.Println("Completed tagging ElastiCache resources")
}
