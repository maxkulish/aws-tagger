package tagger

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
)

// tagVPCResources tags VPC resources according to MAP 2.0 rules
func (t *AWSResourceTagger) tagVPCResources() {
	log.Println("Tagging VPC resources according to MAP 2.0 rules...")

	// Tag Transit Gateway and its attachments
	t.tagTransitGatewayResources()

	// Tag VPC Lattice resources (for plans after 10-May-2024)
	t.tagVPCLatticeResources()

	log.Println("Completed tagging VPC resources")
}

// tagTransitGatewayResources tags Transit Gateway and its attachments
func (t *AWSResourceTagger) tagTransitGatewayResources() {
	log.Println("Tagging Transit Gateway resources...")
	client := ec2.NewFromConfig(t.cfg)

	// Tag Transit Gateways
	tgws, err := client.DescribeTransitGateways(t.ctx, &ec2.DescribeTransitGatewaysInput{})
	if err != nil {
		t.handleError(err, "all", "Transit Gateways")
		return
	}

	for _, tgw := range tgws.TransitGateways {
		// Tag the Transit Gateway itself
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{aws.ToString(tgw.TransitGatewayId)},
			Tags:      t.convertToEC2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(tgw.TransitGatewayId), "Transit Gateway")
			continue
		}
		log.Printf("Successfully tagged Transit Gateway: %s", aws.ToString(tgw.TransitGatewayId))

		// Tag VPN attachments
		t.tagTransitGatewayVPNAttachments(client, aws.ToString(tgw.TransitGatewayId))

		// Tag VPC attachments
		t.tagTransitGatewayVPCAttachments(client, aws.ToString(tgw.TransitGatewayId))

		// Tag Peering attachments
		t.tagTransitGatewayPeeringAttachments(client, aws.ToString(tgw.TransitGatewayId))

		// Tag Direct Connect attachments
		t.tagTransitGatewayDirectConnectAttachments(client, aws.ToString(tgw.TransitGatewayId))
	}
}

// tagTransitGatewayVPNAttachments tags Transit Gateway VPN attachments
func (t *AWSResourceTagger) tagTransitGatewayVPNAttachments(client *ec2.Client, tgwID string) {
	attachments, err := client.DescribeTransitGatewayAttachments(t.ctx, &ec2.DescribeTransitGatewayAttachmentsInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("transit-gateway-id"),
				Values: []string{tgwID},
			},
			{
				Name:   aws.String("resource-type"),
				Values: []string{"vpn"},
			},
		},
	})
	if err != nil {
		t.handleError(err, tgwID, "Transit Gateway VPN Attachments")
		return
	}

	for _, attachment := range attachments.TransitGatewayAttachments {
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{aws.ToString(attachment.TransitGatewayAttachmentId)},
			Tags:      t.convertToEC2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(attachment.TransitGatewayAttachmentId), "Transit Gateway VPN Attachment")
			continue
		}
		log.Printf("Successfully tagged Transit Gateway VPN attachment: %s", aws.ToString(attachment.TransitGatewayAttachmentId))
	}
}

// tagTransitGatewayVPCAttachments tags Transit Gateway VPC attachments
func (t *AWSResourceTagger) tagTransitGatewayVPCAttachments(client *ec2.Client, tgwID string) {
	attachments, err := client.DescribeTransitGatewayAttachments(t.ctx, &ec2.DescribeTransitGatewayAttachmentsInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("transit-gateway-id"),
				Values: []string{tgwID},
			},
			{
				Name:   aws.String("resource-type"),
				Values: []string{"vpc"},
			},
		},
	})
	if err != nil {
		t.handleError(err, tgwID, "Transit Gateway VPC Attachments")
		return
	}

	for _, attachment := range attachments.TransitGatewayAttachments {
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{aws.ToString(attachment.TransitGatewayAttachmentId)},
			Tags:      t.convertToEC2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(attachment.TransitGatewayAttachmentId), "Transit Gateway VPC Attachment")
			continue
		}
		log.Printf("Successfully tagged Transit Gateway VPC attachment: %s", aws.ToString(attachment.TransitGatewayAttachmentId))
	}
}

// tagTransitGatewayPeeringAttachments tags Transit Gateway peering attachments
func (t *AWSResourceTagger) tagTransitGatewayPeeringAttachments(client *ec2.Client, tgwID string) {
	attachments, err := client.DescribeTransitGatewayPeeringAttachments(t.ctx, &ec2.DescribeTransitGatewayPeeringAttachmentsInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("transit-gateway-id"),
				Values: []string{tgwID},
			},
		},
	})
	if err != nil {
		t.handleError(err, tgwID, "Transit Gateway Peering Attachments")
		return
	}

	for _, attachment := range attachments.TransitGatewayPeeringAttachments {
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{aws.ToString(attachment.TransitGatewayAttachmentId)},
			Tags:      t.convertToEC2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(attachment.TransitGatewayAttachmentId), "Transit Gateway Peering Attachment")
			continue
		}
		log.Printf("Successfully tagged Transit Gateway peering attachment: %s", aws.ToString(attachment.TransitGatewayAttachmentId))
	}
}

// tagTransitGatewayDirectConnectAttachments tags Transit Gateway Direct Connect attachments
func (t *AWSResourceTagger) tagTransitGatewayDirectConnectAttachments(client *ec2.Client, tgwID string) {
	attachments, err := client.DescribeTransitGatewayAttachments(t.ctx, &ec2.DescribeTransitGatewayAttachmentsInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("transit-gateway-id"),
				Values: []string{tgwID},
			},
			{
				Name:   aws.String("resource-type"),
				Values: []string{"direct-connect-gateway"},
			},
		},
	})
	if err != nil {
		t.handleError(err, tgwID, "Transit Gateway Direct Connect Attachments")
		return
	}

	for _, attachment := range attachments.TransitGatewayAttachments {
		_, err := client.CreateTags(t.ctx, &ec2.CreateTagsInput{
			Resources: []string{aws.ToString(attachment.TransitGatewayAttachmentId)},
			Tags:      t.convertToEC2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(attachment.TransitGatewayAttachmentId), "Transit Gateway Direct Connect Attachment")
			continue
		}
		log.Printf("Successfully tagged Transit Gateway Direct Connect attachment: %s", aws.ToString(attachment.TransitGatewayAttachmentId))
	}
}

// tagVPCLatticeResources tags VPC Lattice resources (for plans after 10-May-2024)
func (t *AWSResourceTagger) tagVPCLatticeResources() {
	log.Println("Tagging VPC Lattice resources...")
	client := vpclattice.NewFromConfig(t.cfg)

	// Tag Service Networks
	networks, err := client.ListServiceNetworks(t.ctx, &vpclattice.ListServiceNetworksInput{})
	if err != nil {
		t.handleError(err, "all", "VPC Lattice Service Networks")
		return
	}

	for _, network := range networks.Items {
		_, err := client.TagResource(t.ctx, &vpclattice.TagResourceInput{
			ResourceArn: network.Arn,
			Tags:        t.tags, // Using the map[string]string directly
		})
		if err != nil {
			t.handleError(err, aws.ToString(network.Name), "VPC Lattice Service Network")
			continue
		}
		log.Printf("Successfully tagged VPC Lattice service network: %s", aws.ToString(network.Name))
	}

	// Tag Services
	services, err := client.ListServices(t.ctx, &vpclattice.ListServicesInput{})
	if err != nil {
		t.handleError(err, "all", "VPC Lattice Services")
		return
	}

	for _, service := range services.Items {
		_, err := client.TagResource(t.ctx, &vpclattice.TagResourceInput{
			ResourceArn: service.Arn,
			Tags:        t.tags, // Using the map[string]string directly
		})
		if err != nil {
			t.handleError(err, aws.ToString(service.Name), "VPC Lattice Service")
			continue
		}
		log.Printf("Successfully tagged VPC Lattice service: %s", aws.ToString(service.Name))
	}
}

// convertToEC2Tags converts the common tags map to EC2-specific tags
func (t *AWSResourceTagger) convertToEC2Tags() []types.Tag {
	ec2Tags := make([]types.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		ec2Tags = append(ec2Tags, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return ec2Tags
}
