package tagger

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

// tagEC2Resources tags EC2 instances and related resources
func (t *AWSResourceTagger) tagEC2Resources() {
	client := ec2.NewFromConfig(t.cfg)

	// Tag EC2 instances
	paginator := ec2.NewDescribeInstancesPaginator(client, &ec2.DescribeInstancesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error describing EC2 instances: %v", err)
			continue
		}

		for _, reservation := range page.Reservations {
			for _, instance := range reservation.Instances {
				_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
					Resources: []string{*instance.InstanceId},
					Tags:      t.awsTags,
				})
				if err != nil {
					t.handleError(err, *instance.InstanceId, "EC2")
					continue
				}
				log.Printf("Tagged EC2 instance: %s", *instance.InstanceId)
			}
		}
	}

	// Tag EBS volumes
	volPaginator := ec2.NewDescribeVolumesPaginator(client, &ec2.DescribeVolumesInput{})
	for volPaginator.HasMorePages() {
		page, err := volPaginator.NextPage(t.ctx)
		if err != nil {
			log.Printf("Error describing EBS volumes: %v", err)
			continue
		}

		for _, volume := range page.Volumes {
			_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
				Resources: []string{*volume.VolumeId},
				Tags:      t.awsTags,
			})
			if err != nil {
				t.handleError(err, *volume.VolumeId, "EBS")
				continue
			}
			log.Printf("Tagged EBS volume: %s", *volume.VolumeId)
		}
	}
}
