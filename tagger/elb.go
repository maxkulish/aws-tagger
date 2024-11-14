package tagger

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbTypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2Types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
)

// tagELBResources tags both Classic and Application/Network Load Balancers
func (t *AWSResourceTagger) tagELBResources() {
	log.Println("Tagging ELB resources...")

	// Tag Classic Load Balancers
	t.tagClassicLoadBalancers()

	// Tag Application and Network Load Balancers
	t.tagApplicationAndNetworkLoadBalancers()

	log.Println("Completed tagging ELB resources")
}

// tagClassicLoadBalancers tags Classic Load Balancers
func (t *AWSResourceTagger) tagClassicLoadBalancers() {
	client := elasticloadbalancing.NewFromConfig(t.cfg)

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
			Tags:             t.convertToClassicELBTags(),
		})
		if err != nil {
			t.handleError(err, lbName, "Classic Load Balancer")
			continue
		}
		log.Printf("Successfully tagged Classic Load Balancer: %s", lbName)
	}
}

// tagApplicationAndNetworkLoadBalancers tags Application and Network Load Balancers
func (t *AWSResourceTagger) tagApplicationAndNetworkLoadBalancers() {
	client := elasticloadbalancingv2.NewFromConfig(t.cfg)

	// List ALB/NLB Load Balancers
	input := &elasticloadbalancingv2.DescribeLoadBalancersInput{}
	result, err := client.DescribeLoadBalancers(t.ctx, input)
	if err != nil {
		t.handleError(err, "all", "ALB/NLB Load Balancers")
		return
	}

	for _, lb := range result.LoadBalancers {
		lbArn := aws.ToString(lb.LoadBalancerArn)

		_, err := client.AddTags(t.ctx, &elasticloadbalancingv2.AddTagsInput{
			ResourceArns: []string{lbArn},
			Tags:        t.convertToELBv2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(lb.LoadBalancerName), "ALB/NLB Load Balancer")
			continue
		}
		log.Printf("Successfully tagged %s Load Balancer: %s", lb.Type, aws.ToString(lb.LoadBalancerName))

		// Tag target groups associated with this load balancer
		t.tagTargetGroups(client, lbArn)
	}
}

// tagTargetGroups tags target groups associated with ALB/NLB
func (t *AWSResourceTagger) tagTargetGroups(client *elasticloadbalancingv2.Client, lbArn string) {
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
			Tags:        t.convertToELBv2Tags(),
		})
		if err != nil {
			t.handleError(err, aws.ToString(tg.TargetGroupName), "Target Group")
			continue
		}
		log.Printf("Successfully tagged Target Group: %s", aws.ToString(tg.TargetGroupName))
	}
}

// convertToClassicELBTags converts common tags to Classic ELB tags
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

// convertToELBv2Tags converts common tags to ELBv2 tags
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
