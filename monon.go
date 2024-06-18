import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
)

// importJSONTransaction demonstrates loading data into a BigQuery table from a GCS JSON file
// using transaction API.
func importJSONTransaction(w io.Writer, projectID, gcsURI string) error {
	// projectID := "my-project-id"
	// gcsURI := "gs://cloud-samples-data/bigquery/us-states/us-states.json"
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.SourceFormat = bigquery.JSON
	loader := client.Dataset("us_states").Table("us_states").LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	// Run the job with transaction API.
	job, err := loader.Run(ctx, &bigquery.JobOptions{
		// Set the job's label to "example-label".
		Labels: map[string]string{"example-label": "example-value"},
	})
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %w", status.Err())
	}
	fmt.Fprintf(w, "Transaction job completed.\n")
	return nil
}
  
