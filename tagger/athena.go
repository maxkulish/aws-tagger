package tagger

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// tagAthenaWorkgroups tags Athena workgroups
func (t *AWSResourceTagger) tagAthenaWorkgroups(client *athena.Client) {
	input := &athena.ListWorkGroupsInput{}
	for {
		workgroups, err := client.ListWorkGroups(t.ctx, input)
		if err != nil {
			t.handleError(err, "all", "Athena Workgroups")
			return
		}

		for _, workgroup := range workgroups.WorkGroups {
			wgName := aws.ToString(workgroup.Name)
			if wgName == "primary" {
				continue
			}

			arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s",
				t.cfg.Region, t.accountID, wgName)

			_, err = client.TagResource(t.ctx, &athena.TagResourceInput{
				ResourceARN: aws.String(arn),
				Tags:        t.convertToAthenaTags(),
			})
			if err != nil {
				t.handleError(err, wgName, "Athena Workgroup")
				continue
			}
			log.Printf("Successfully tagged Athena workgroup: %s", wgName)
		}

		if workgroups.NextToken == nil {
			break
		}
		input.NextToken = workgroups.NextToken
	}
}

// tagAthenaDataCatalogs tags Athena data catalogs
func (t *AWSResourceTagger) tagAthenaDataCatalogs(client *athena.Client) {
	input := &athena.ListDataCatalogsInput{}
	for {
		catalogs, err := client.ListDataCatalogs(t.ctx, input)
		if err != nil {
			t.handleError(err, "all", "Athena Data Catalogs")
			return
		}

		for _, catalog := range catalogs.DataCatalogsSummary {
			catalogName := aws.ToString(catalog.CatalogName)
			if catalogName == "AwsDataCatalog" {
				continue
			}

			arn := fmt.Sprintf("arn:aws:athena:%s:%s:datacatalog/%s",
				t.cfg.Region, t.accountID, catalogName)

			_, err = client.TagResource(t.ctx, &athena.TagResourceInput{
				ResourceARN: aws.String(arn),
				Tags:        t.convertToAthenaTags(),
			})
			if err != nil {
				t.handleError(err, catalogName, "Athena Data Catalog")
				continue
			}
			log.Printf("Successfully tagged Athena data catalog: %s", catalogName)
		}

		if catalogs.NextToken == nil {
			break
		}
		input.NextToken = catalogs.NextToken
	}
}

// tagAthenaPreparedStatements tags Athena prepared statements
func (t *AWSResourceTagger) tagAthenaPreparedStatements(client *athena.Client) {
	workgroups, err := client.ListWorkGroups(t.ctx, &athena.ListWorkGroupsInput{})
	if err != nil {
		t.handleError(err, "all", "Athena Workgroups for Prepared Statements")
		return
	}

	for _, workgroup := range workgroups.WorkGroups {
		wgName := aws.ToString(workgroup.Name)
		input := &athena.ListPreparedStatementsInput{
			WorkGroup: aws.String(wgName),
		}

		for {
			statements, err := client.ListPreparedStatements(t.ctx, input)
			if err != nil {
				t.handleError(err, wgName, "Athena Prepared Statements")
				continue
			}

			for _, statement := range statements.PreparedStatements {
				statementName := aws.ToString(statement.StatementName)
				arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s/preparedstatement/%s",
					t.cfg.Region, t.accountID, wgName, statementName)

				_, err = client.TagResource(t.ctx, &athena.TagResourceInput{
					ResourceARN: aws.String(arn),
					Tags:        t.convertToAthenaTags(),
				})
				if err != nil {
					t.handleError(err, statementName, "Athena Prepared Statement")
					continue
				}
				log.Printf("Successfully tagged Athena prepared statement: %s in workgroup %s",
					statementName, wgName)
			}

			if statements.NextToken == nil {
				break
			}
			input.NextToken = statements.NextToken
		}
	}
}

// tagAthenaQueryExecutions tags Athena query executions
func (t *AWSResourceTagger) tagAthenaQueryExecutions(client *athena.Client) {
	workgroups, err := client.ListWorkGroups(t.ctx, &athena.ListWorkGroupsInput{})
	if err != nil {
		t.handleError(err, "all", "Athena Workgroups for Query Executions")
		return
	}

	for _, workgroup := range workgroups.WorkGroups {
		wgName := aws.ToString(workgroup.Name)
		input := &athena.ListQueryExecutionsInput{
			WorkGroup: aws.String(wgName),
		}

		for {
			executions, err := client.ListQueryExecutions(t.ctx, input)
			if err != nil {
				t.handleError(err, wgName, "Athena Query Executions")
				continue
			}

			for _, queryID := range executions.QueryExecutionIds {
				arn := fmt.Sprintf("arn:aws:athena:%s:%s:workgroup/%s/query/%s",
					t.cfg.Region, t.accountID, wgName, queryID)

				_, err = client.TagResource(t.ctx, &athena.TagResourceInput{
					ResourceARN: aws.String(arn),
					Tags:        t.convertToAthenaTags(),
				})
				if err != nil {
					t.handleError(err, queryID, "Athena Query Execution")
					continue
				}
				log.Printf("Successfully tagged Athena query execution: %s in workgroup %s",
					queryID, wgName)
			}

			if executions.NextToken == nil {
				break
			}
			input.NextToken = executions.NextToken
		}
	}
}

// convertToAthenaTags converts the common tags map to Athena-specific tags
func (t *AWSResourceTagger) convertToAthenaTags() []athenatypes.Tag {
	athenaTags := make([]athenatypes.Tag, 0, len(t.tags))
	for k, v := range t.tags {
		athenaTags = append(athenaTags, athenatypes.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		})
	}
	return athenaTags
}

// tagAthenaResources tags all Athena resources
func (t *AWSResourceTagger) tagAthenaResources() {
	log.Println("Tagging Athena resources...")
	client := athena.NewFromConfig(t.cfg)

	t.tagAthenaWorkgroups(client)
	t.tagAthenaDataCatalogs(client)
	t.tagAthenaPreparedStatements(client)
	t.tagAthenaQueryExecutions(client)

	log.Println("Completed tagging Athena resources")
}
