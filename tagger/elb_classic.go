package tagger

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbTypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2Types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
)

// ClassicELBAPI interface for Classic ELB client operations
type ClassicELBAPI interface {
	DescribeLoadBalancers(ctx context.Context, params *elasticloadbalancing.DescribeLoadBalancersInput, optFns ...func(*elasticloadbalancing.Options)) (*elasticloadbalancing.DescribeLoadBalancersOutput, error)
	AddTags(ctx context.Context, params *elasticloadbalancing.AddTagsInput, optFns ...func(*elasticloadbalancing.Options)) (*elasticloadbalancing.AddTagsOutput, error)
}

// ELBv2API interface for Application/Network Load Balancer client operations
type ELBv2API interface {
	DescribeLoadBalancers(ctx context.Context, params *elasticloadbalancingv2.DescribeLoadBalancersInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.DescribeLoadBalancersOutput, error)
	DescribeTargetGroups(ctx context.Context, params *elasticloadbalancingv2.DescribeTargetGroupsInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.DescribeTargetGroupsOutput, error)
	AddTags(ctx context.Context, params *elasticloadbalancingv2.AddTagsInput, optFns ...func(*elasticloadbalancingv2.Options)) (*elasticloadbalancingv2.AddTagsOutput, error)
}

// tagELBResources creates clients and initiates the tagging process
func (t *AWSResourceTagger) tagELBResources() {
	classicClient := elasticloadbalancing.NewFromConfig(t.cfg)
	v2Client := elasticloadbalancingv2.NewFromConfig(t.cfg)

	t.tagELBResourcesWithClients(classicClient, v2Client)
}

// tagELBResourcesWithClients tags both Classic and Application/Network Load Balancers
func (t *AWSResourceTagger) tagELBResourcesWithClients(classicClient ClassicELBAPI, v2Client ELBv2API) {
	log.Println("Tagging ELB resources...")
	defer log.Println("Completed tagging ELB resources")

	if len(t.tags) == 0 {
		log.Println("No tags provided, skipping ELB resource tagging")
		return
	}

	// Tag Classic Load Balancers
	t.tagClassicLoadBalancersWithClient(classicClient)

	// Tag Application and Network Load Balancers
	t.tagApplicationAndNetworkLoadBalancersWithClient(v2Client)
}

// tagClassicLoadBalancersWithClient tags Classic Load Balancers
func (t *AWSResourceTagger) tagClassicLoadBalancersWithClient(client ClassicELBAPI) {
	// List Classic Load Balancers
	input := &elasticloadbalancing.DescribeLoadBalancersInput{}
	result, err := client.DescribeLoadBalancers(t.ctx, input)
	if err != nil {
		t.handleError(err, "all", "Classic Load Balancers")
		return
	}

	for _, lb := range result.LoadBalancerDescriptions {
		lbName := aws.ToString(lb.LoadBalancerName)

		_, err := client.AddTags(t.ctx, &elasticloadbalancing.AddTagsInput{
			LoadBalancerNames: []string{lbName},
			Tags:              t.convertToClassicELBTags(),
		})
		if err != nil {
			t.handleError(err, lbName, "Classic Load Balancer")
			continue
		}
		log.Printf("Successfully tagged Classic Load Balancer: %s", lbName)
	}
}

// tagApplicationAndNetworkLoadBalancersWithClient handles ALB/NLB resource tagging
func (t *AWSResourceTagger) tagApplicationAndNetworkLoadBalancersWithClient(client ELBv2API) {
	// List all Application and Network Load Balancers
	loadBalancers, err := t.listLoadBalancers(client)
	if err != nil {
		return // Error already logged in listLoadBalancers
	}

	for _, lb := range loadBalancers {
		// Tag each load balancer
		if err := t.tagLoadBalancer(client, lb); err != nil {
			// Continue to next load balancer if tagging fails
			continue
		}

		// Tag target groups for successfully tagged load balancer
		t.tagTargetGroupsForLoadBalancer(client, lb)
	}
}

// listLoadBalancers gets all ALB/NLB load balancers
func (t *AWSResourceTagger) listLoadBalancers(client ELBv2API) ([]elbv2Types.LoadBalancer, error) {
	result, err := client.DescribeLoadBalancers(t.ctx, &elasticloadbalancingv2.DescribeLoadBalancersInput{})
	if err != nil {
		t.handleError(err, "all", "ALB/NLB Load Balancers")
		return nil, err
	}
	return result.LoadBalancers, nil
}

// tagLoadBalancer tags a single ALB/NLB
func (t *AWSResourceTagger) tagLoadBalancer(client ELBv2API, lb elbv2Types.LoadBalancer) error {
	lbName := aws.ToString(lb.LoadBalancerName)
	lbArn := aws.ToString(lb.LoadBalancerArn)

	_, err := client.AddTags(t.ctx, &elasticloadbalancingv2.AddTagsInput{
		ResourceArns: []string{lbArn},
		Tags:         t.convertToELBv2Tags(),
	})
	if err != nil {
		t.handleError(err, lbName, "ALB/NLB Load Balancer")
		return err
	}

	var lbType string
	switch lb.Type {
	case elbv2Types.LoadBalancerTypeEnumApplication:
		lbType = "application"
	case elbv2Types.LoadBalancerTypeEnumNetwork:
		lbType = "network"
	case elbv2Types.LoadBalancerTypeEnumGateway:
		lbType = "gateway"
	default:
		lbType = string(lb.Type)
	}

	log.Printf("Successfully tagged %s Load Balancer: %s", lbType, lbName)
	return nil
}

// tagTargetGroupsForLoadBalancer tags all target groups associated with a load balancer
func (t *AWSResourceTagger) tagTargetGroupsForLoadBalancer(client ELBv2API, lb elbv2Types.LoadBalancer) {
	lbArn := aws.ToString(lb.LoadBalancerArn)
	targetGroups, err := client.DescribeTargetGroups(t.ctx, &elasticloadbalancingv2.DescribeTargetGroupsInput{
		LoadBalancerArn: aws.String(lbArn),
	})
	if err != nil {
		t.handleError(err, lbArn, "Target Groups")
		return
	}

	for _, tg := range targetGroups.TargetGroups {
		if err := t.tagTargetGroup(client, tg); err != nil {
			// Continue to next target group if tagging fails
			continue
		}
	}
}

// tagTargetGroup tags a single target group
func (t *AWSResourceTagger) tagTargetGroup(client ELBv2API, tg elbv2Types.TargetGroup) error {
	tgName := aws.ToString(tg.TargetGroupName)
	tgArn := aws.ToString(tg.TargetGroupArn)

	_, err := client.AddTags(t.ctx, &elasticloadbalancingv2.AddTagsInput{
		ResourceArns: []string{tgArn},
		Tags:         t.convertToELBv2Tags(),
	})
	if err != nil {
		t.handleError(err, tgName, "Target Group")
		return err
	}

	log.Printf("Successfully tagged Target Group: %s", tgName)
	return nil
}

// tagTargetGroupsWithClient tags target groups associated with ALB/NLB
func (t *AWSResourceTagger) tagTargetGroupsWithClient(client ELBv2API, lbArn string) {
	input := &elasticloadbalancingv2.DescribeTargetGroupsInput{
		LoadBalancerArn: aws.String(lbArn),
	}

	targetGroups, err := client.DescribeTargetGroups(t.ctx, input)
	if err != nil {
		t.handleError(err, lbArn, "Target Groups")
		return
	}

	for _, tg := range targetGroups.TargetGroups {
		tgArn := aws.ToString(tg.TargetGroupArn)

		_, err := client.AddTags(t.ctx, &elasticloadbalancingv2.AddTagsInput{
			ResourceArns: []string{tgArn},
			Tags:         t.convertToELBv2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(tg.TargetGroupName), "Target Group")
			continue
		}
		log.Printf("Successfully tagged Target Group: %s", aws.ToString(tg.TargetGroupName))
	}
}

// Helper functions remain unchanged
func (t *AWSResourceTagger) convertToClassicELBTags() []elbTypes.Tag {
	elbTags := make([]elbTypes.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		elbTags = append(elbTags, elbTypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return elbTags
}

func (t *AWSResourceTagger) convertToELBv2Tags() []elbv2Types.Tag {
	elbTags := make([]elbv2Types.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		elbTags = append(elbTags, elbv2Types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return elbTags
}
