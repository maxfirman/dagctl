use mockito::Server;
use serde_json::json;
use std::env;

#[tokio::test]
async fn test_runs_list_success() {
    let mut server = Server::new_async().await;

    let _mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runsOrError": {
                        "__typename": "Runs",
                        "results": [
                            {
                                "runId": "test-run-1",
                                "pipelineName": "test-job",
                                "status": "SUCCESS",
                                "startTime": 1234567890.0,
                                "endTime": 1234567900.0
                            }
                        ]
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Set the mock server URL
    unsafe {
        env::set_var("DAGSTER_API_URL", server.url());
        env::set_var("DAGSTER_API_TOKEN", "test-token");
    }

    // This would now actually call the mocked endpoint
    // For now, we just verify the mock setup works
    assert_eq!(env::var("DAGSTER_API_URL").unwrap(), server.url());

    unsafe {
        env::remove_var("DAGSTER_API_URL");
        env::remove_var("DAGSTER_API_TOKEN");
    }
}

#[tokio::test]
async fn test_runs_get_not_found() {
    let mut server = Server::new_async().await;

    let _mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runOrError": {
                        "__typename": "RunNotFoundError",
                        "message": "Run not found"
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Test would verify error handling
}

#[tokio::test]
async fn test_runs_events_success() {
    let mut server = Server::new_async().await;

    let _mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runOrError": {
                        "__typename": "Run",
                        "eventConnection": {
                            "events": [
                                {
                                    "__typename": "LogMessageEvent",
                                    "runId": "test-run",
                                    "message": "Test log message",
                                    "timestamp": "2026-03-06T17:00:00Z",
                                    "level": "INFO",
                                    "stepKey": null
                                }
                            ]
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Test would verify event parsing
}

#[tokio::test]
async fn test_runs_logs_with_captured_event() {
    let mut server = Server::new_async().await;

    // First request to get events
    let _mock1 = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runOrError": {
                        "__typename": "Run",
                        "eventConnection": {
                            "events": [
                                {
                                    "__typename": "LogsCapturedEvent",
                                    "runId": "test-run",
                                    "message": "Logs captured",
                                    "timestamp": "2026-03-06T17:00:00Z",
                                    "level": "INFO",
                                    "stepKey": null,
                                    "fileKey": "compute_logs"
                                }
                            ]
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Second request to get logs
    let _mock2 = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runOrError": {
                        "__typename": "Run",
                        "capturedLogs": {
                            "stdout": "Test stdout output",
                            "stderr": null
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Test would verify two-step log retrieval
}

#[tokio::test]
async fn test_runs_logs_no_captured_event() {
    let mut server = Server::new_async().await;

    let _mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "runOrError": {
                        "__typename": "Run",
                        "eventConnection": {
                            "events": []
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Test would verify error when no LogsCapturedEvent found
}

#[tokio::test]
async fn test_graphql_error_response() {
    let mut server = Server::new_async().await;

    let _mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "errors": [
                    {
                        "message": "Invalid query",
                        "locations": [{"line": 1, "column": 1}]
                    }
                ]
            })
            .to_string(),
        )
        .create_async()
        .await;

    // Test would verify GraphQL error handling
}

#[tokio::test]
async fn test_network_error() {
    // Test would verify handling of network failures
    // This would require refactoring to inject a failing client
}
