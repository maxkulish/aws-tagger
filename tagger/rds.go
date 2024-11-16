package tagger

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// RDSAPI interface for RDS client operations
type RDSAPI interface {
	DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	DescribeDBClusters(ctx context.Context, params *rds.DescribeDBClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClustersOutput, error)
	DescribeDBSnapshots(ctx context.Context, params *rds.DescribeDBSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBSnapshotsOutput, error)
	DescribeDBClusterSnapshots(ctx context.Context, params *rds.DescribeDBClusterSnapshotsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBClusterSnapshotsOutput, error)
	AddTagsToResource(ctx context.Context, params *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error)
}

// tagRDSResources is the main entry point that creates and uses the client
func (t *AWSResourceTagger) tagRDSResources() {
	fmt.Println("=====================================")
	log.Println("Tagging RDS resources...")

	client := rds.NewFromConfig(t.cfg)
	t.tagRDSResourcesWithClient(client)

	log.Println("Completed tagging RDS resources")
}

// tagRDSResourcesWithClient handles the actual tagging logic with a provided client
func (t *AWSResourceTagger) tagRDSResourcesWithClient(client RDSAPI) {
	t.tagDBInstancesWithClient(client)
	t.tagDBClustersWithClient(client)
	t.tagDBSnapshotsWithClient(client)
	t.tagClusterSnapshotsWithClient(client)
}

// tagDBInstancesWithClient tags RDS DB instances
func (t *AWSResourceTagger) tagDBInstancesWithClient(client RDSAPI) {
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

// tagDBClustersWithClient tags RDS DB clusters
func (t *AWSResourceTagger) tagDBClustersWithClient(client RDSAPI) {
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

// tagDBSnapshotsWithClient tags RDS DB snapshots
func (t *AWSResourceTagger) tagDBSnapshotsWithClient(client RDSAPI) {
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

// tagClusterSnapshotsWithClient tags RDS cluster snapshots
func (t *AWSResourceTagger) tagClusterSnapshotsWithClient(client RDSAPI) {
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
