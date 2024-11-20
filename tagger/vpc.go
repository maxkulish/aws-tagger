package tagger

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice"
)

// VPCEC2API interface for VPC EC2 client operations
type VPCEC2API interface {
	DescribeTransitGateways(ctx context.Context, params *ec2.DescribeTransitGatewaysInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewaysOutput, error)
	DescribeTransitGatewayAttachments(ctx context.Context, params *ec2.DescribeTransitGatewayAttachmentsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewayAttachmentsOutput, error)
	DescribeTransitGatewayPeeringAttachments(ctx context.Context, params *ec2.DescribeTransitGatewayPeeringAttachmentsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeTransitGatewayPeeringAttachmentsOutput, error)
	CreateTags(ctx context.Context, params *ec2.CreateTagsInput, optFns ...func(*ec2.Options)) (*ec2.CreateTagsOutput, error)
}

// VPCLatticeAPI interface for VPC Lattice client operations
type VPCLatticeAPI interface {
	ListServiceNetworks(ctx context.Context, params *vpclattice.ListServiceNetworksInput, optFns ...func(*vpclattice.Options)) (*vpclattice.ListServiceNetworksOutput, error)
	ListServices(ctx context.Context, params *vpclattice.ListServicesInput, optFns ...func(*vpclattice.Options)) (*vpclattice.ListServicesOutput, error)
	TagResource(ctx context.Context, params *vpclattice.TagResourceInput, optFns ...func(*vpclattice.Options)) (*vpclattice.TagResourceOutput, error)
}

// tagVPCResources is the main entry point that creates and uses the clients
func (t *AWSResourceTagger) tagVPCResources() {
	ec2Client := ec2.NewFromConfig(t.cfg)
	latticeClient := vpclattice.NewFromConfig(t.cfg)
	t.tagVPCResourcesWithClients(ec2Client, latticeClient)
}

// tagVPCResourcesWithClients handles the actual tagging logic with provided clients
func (t *AWSResourceTagger) tagVPCResourcesWithClients(ec2Client VPCEC2API, latticeClient VPCLatticeAPI) {
	fmt.Println("=====================================")
	log.Println("Tagging VPC resources according to MAP 2.0 rules...")

	// Tag Transit Gateway and its attachments
	t.tagTransitGatewayResourcesWithClient(ec2Client)

	// Tag VPC Lattice resources
	t.tagVPCLatticeResourcesWithClient(latticeClient)

	log.Println("Completed tagging VPC resources")
}

// tagTransitGatewayResourcesWithClient tags Transit Gateway resources with provided client
func (t *AWSResourceTagger) tagTransitGatewayResourcesWithClient(client VPCEC2API) {
	log.Println("Tagging Transit Gateway resources...")

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

// tagVPCLatticeResourcesWithClient tags VPC Lattice resources with provided client
func (t *AWSResourceTagger) tagVPCLatticeResourcesWithClient(client VPCLatticeAPI) {
	log.Println("Tagging VPC Lattice resources...")

	// Tag Service Networks
	networks, err := client.ListServiceNetworks(t.ctx, &vpclattice.ListServiceNetworksInput{})
	if err != nil {
		t.handleError(err, "all", "VPC Lattice Service Networks")
		return
	}

	for _, network := range networks.Items {
		_, err := client.TagResource(t.ctx, &vpclattice.TagResourceInput{
			ResourceArn: network.Arn,
			Tags:        t.tags,
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
			Tags:        t.tags,
		})
		if err != nil {
			t.handleError(err, aws.ToString(service.Name), "VPC Lattice Service")
			continue
		}
		log.Printf("Successfully tagged VPC Lattice service: %s", aws.ToString(service.Name))
	}
}

// tagTransitGatewayVPNAttachments tags Transit Gateway VPN attachments
func (t *AWSResourceTagger) tagTransitGatewayVPNAttachments(client VPCEC2API, tgwID string) {
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
func (t *AWSResourceTagger) tagTransitGatewayVPCAttachments(client VPCEC2API, tgwID string) {
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
func (t *AWSResourceTagger) tagTransitGatewayPeeringAttachments(client VPCEC2API, tgwID string) {
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
func (t *AWSResourceTagger) tagTransitGatewayDirectConnectAttachments(client VPCEC2API, tgwID string) {
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
