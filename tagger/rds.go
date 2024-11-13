package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// tagRDSResources tags RDS instances, clusters, snapshots, and other RDS resources
func (t *AWSResourceTagger) tagRDSResources() {
	fmt.Println("=====================================")
	log.Println("Tagging RDS resources...")
	client := rds.NewFromConfig(t.cfg)

	// Tag DB instances
	t.tagDBInstances(client)

	// Tag DB clusters
	t.tagDBClusters(client)

	// Tag DB snapshots
	t.tagDBSnapshots(client)

	// Tag cluster snapshots
	t.tagClusterSnapshots(client)

	log.Println("Completed tagging RDS resources")
}

// tagDBInstances tags RDS DB instances
func (t *AWSResourceTagger) tagDBInstances(client *rds.Client) {
	instances, err := client.DescribeDBInstances(t.ctx, &rds.DescribeDBInstancesInput{})
	if err != nil {
		t.handleError(err, "all", "RDS DB Instances")
		return
	}

	for _, instance := range instances.DBInstances {
		arn := aws.ToString(instance.DBInstanceArn)
		input := &rds.AddTagsToResourceInput{
			ResourceName: instance.DBInstanceArn,
			Tags:         t.convertToRDSTags(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "RDS DB Instance")
			continue
		}
		log.Printf("Successfully tagged RDS instance: %s", aws.ToString(instance.DBInstanceIdentifier))
	}
}

// tagDBClusters tags RDS DB clusters
func (t *AWSResourceTagger) tagDBClusters(client *rds.Client) {
	clusters, err := client.DescribeDBClusters(t.ctx, &rds.DescribeDBClustersInput{})
	if err != nil {
		t.handleError(err, "all", "RDS DB Clusters")
		return
	}

	for _, cluster := range clusters.DBClusters {
		arn := aws.ToString(cluster.DBClusterArn)
		input := &rds.AddTagsToResourceInput{
			ResourceName: cluster.DBClusterArn,
			Tags:         t.convertToRDSTags(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "RDS DB Cluster")
			continue
		}
		log.Printf("Successfully tagged RDS cluster: %s", aws.ToString(cluster.DBClusterIdentifier))
	}
}

// tagDBSnapshots tags RDS DB snapshots
func (t *AWSResourceTagger) tagDBSnapshots(client *rds.Client) {
	snapshots, err := client.DescribeDBSnapshots(t.ctx, &rds.DescribeDBSnapshotsInput{})
	if err != nil {
		t.handleError(err, "all", "RDS DB Snapshots")
		return
	}

	for _, snapshot := range snapshots.DBSnapshots {
		arn := aws.ToString(snapshot.DBSnapshotArn)
		input := &rds.AddTagsToResourceInput{
			ResourceName: snapshot.DBSnapshotArn,
			Tags:         t.convertToRDSTags(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "RDS DB Snapshot")
			continue
		}
		log.Printf("Successfully tagged RDS snapshot: %s", aws.ToString(snapshot.DBSnapshotIdentifier))
	}
}

// tagClusterSnapshots tags RDS cluster snapshots
func (t *AWSResourceTagger) tagClusterSnapshots(client *rds.Client) {
	snapshots, err := client.DescribeDBClusterSnapshots(t.ctx, &rds.DescribeDBClusterSnapshotsInput{})
	if err != nil {
		t.handleError(err, "all", "RDS Cluster Snapshots")
		return
	}

	for _, snapshot := range snapshots.DBClusterSnapshots {
		arn := aws.ToString(snapshot.DBClusterSnapshotArn)
		input := &rds.AddTagsToResourceInput{
			ResourceName: snapshot.DBClusterSnapshotArn,
			Tags:         t.convertToRDSTags(),
		}

		_, err := client.AddTagsToResource(t.ctx, input)
		if err != nil {
			t.handleError(err, arn, "RDS Cluster Snapshot")
			continue
		}
		log.Printf("Successfully tagged RDS cluster snapshot: %s", aws.ToString(snapshot.DBClusterSnapshotIdentifier))
	}
}

// convertToRDSTags converts the common tags map to RDS-specific tags
func (t *AWSResourceTagger) convertToRDSTags() []rdstypes.Tag {
	rdsTags := make([]rdstypes.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		rdsTags = append(rdsTags, rdstypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return rdsTags
}
