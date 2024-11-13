package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/maxkulish/aws-tagger/tagger"
)

// Configuration defaults
const (
	defaultProfile  = "default"
	defaultRegion   = "us-east-1"
	defaultTagValue = "mig12345"
	defaultTagKey   = "map-migrated"
)

// MapTags represents the required MAP 2.0 tags
var mapTags = map[string]string{}

// CLIFlags holds the command-line arguments
type CLIFlags struct {
	profile     string
	region      string
	mapKeyValue string
	tags        string
}

// validateTags checks if the tags string is properly formatted
func validateTags(tagsStr string) error {
	if tagsStr == "" {
		return fmt.Errorf("--tag flag is required. Format: --tag key:value or --tag key1:value1,key2:value2")
	}

	tagPairs := strings.Split(tagsStr, ",")
	for _, pair := range tagPairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid tag format: %s. Each tag must be in key:value format", pair)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return fmt.Errorf("empty key found in tag pair: %s", pair)
		}
		if value == "" {
			return fmt.Errorf("empty value found in tag pair: %s", pair)
		}
	}
	return nil
}

// parseCustomTags parses the custom tags string into a map
func parseCustomTags(tagsStr string) map[string]string {
	tags := make(map[string]string)

	tagPairs := strings.Split(tagsStr, ",")

	for _, pair := range tagPairs {
		parts := strings.SplitN(pair, ":", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		tags[key] = value
	}

	return tags
}

// parseFlags parses the command-line arguments and returns a CLIFlags
func parseFlags() *CLIFlags {
	flags := CLIFlags{}

	flag.StringVar(&flags.profile, "profile", defaultProfile, "AWS profile to use")
	flag.StringVar(&flags.region, "region", defaultRegion, "AWS region to use")
	flag.StringVar(&flags.mapKeyValue, "map-migrated", defaultTagValue, "MAP 2.0 value to use")
	flag.StringVar(&flags.tags, "tag", "", "Custom tags in key:value format (can be comma-separated for multiple tags)")

	// Add aliases for flags
	flag.StringVar(&flags.profile, "p", defaultProfile, "AWS profile to use (shorthand)")
	flag.StringVar(&flags.region, "r", defaultRegion, "AWS region to use (shorthand)")
	flag.StringVar(&flags.tags, "t", "", "Custom tags (shorthand)")

	flag.Parse()

	return &flags
}

func main() {
	flags := parseFlags()

	// Validate tags before proceeding
	if err := validateTags(flags.tags); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	// Log the configuration being used
	log.Printf("Using AWS Profile: %s", flags.profile)
	log.Printf("Using AWS Region: %s", flags.region)

	// Parse custom tags and merge with mapTags
	customTags := parseCustomTags(flags.tags)
	allTags := make(map[string]string)

	// Copy mapTags to allTags
	for k, v := range mapTags {
		allTags[k] = v
	}

	// Merge custom tags (will override mapTags if there are duplicates)
	for k, v := range customTags {
		allTags[k] = v
	}

	// Log the tags being applied
	log.Printf("Tags to be applied: %v", allTags)

	ctx := context.Background()

	tagger, err := tagger.NewAWSResourceTagger(ctx, flags.profile, flags.region, allTags)
	if err != nil {
		log.Fatalf("Failed to create tagger: %v", err)
	}

	tagger.TagAllResources()
}
