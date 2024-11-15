package tagger

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

// EC2API interface for EC2 client operations
type EC2API interface {
	DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DescribeVolumes(ctx context.Context, params *ec2.DescribeVolumesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
	CreateTags(ctx context.Context, params *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error)
}

// tagEC2Resources tags EC2 instances and related resources
func (t *AWSResourceTagger) tagEC2Resources() {
	client := ec2.NewFromConfig(t.cfg)
	t.tagEC2ResourcesWithClient(client)
}

// tagEC2ResourcesWithClient tags EC2 instances and related resources using the provided client
func (t *AWSResourceTagger) tagEC2ResourcesWithClient(client EC2API) {
	var instanceIds []string

	// Describe EC2 instances and collect instance IDs
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error describing EC2 instances: %v", err)
			return // Stop if there's an error during instance description
		}

		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				instanceIds = append(instanceIds, *instance.InstanceId)
			}
		}
	}

	// Tag the collected EC2 instances
	for _, instanceID := range instanceIds {
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{instanceID},
			Tags:      t.awsTags,
		})
		if err != nil {
			t.handleError(err, instanceID, "EC2")
			return // Stop if any instance tagging fails.
		}
		log.Printf("Tagged EC2 instance: %s", instanceID)
	}
	
	// Only proceed to volume tagging if instance processing was successful
	volPaginator := ec2.NewDescribeVolumesPaginator(client, &ec2.DescribeVolumesInput{})
	for volPaginator.HasMorePages() {
		page, err := volPaginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error describing EBS volumes: %v", err)
			continue // Safe to continue if volume description fails.
		}

		for _, volume := range page.Volumes {
			_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
				Resources: []string{*volume.VolumeId},
				Tags:      t.awsTags,
			})
			if err != nil {
				t.handleError(err, *volume.VolumeId, "EBS")
				continue // Safe to continue to the next volume if tagging fails.
			}
			log.Printf("Tagged EBS volume: %s", *volume.VolumeId)
		}
	}
}
