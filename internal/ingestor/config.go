package ingestor

import (
	"context"
	"os"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	_ "github.com/joho/godotenv/autoload"
)

var Ctx = datadog.NewDefaultContext(context.Background())

func InitializeDataDog() *datadog.APIClient {
	var configuration = datadog.NewConfiguration()

	configuration.AddDefaultHeader("DD-APPLICATION-KEY", os.Getenv("DD_APPLICATION_KEY"))

	var apiClient = datadog.NewAPIClient(configuration)

	return apiClient
}
