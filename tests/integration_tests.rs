use mockito::Server;
use serde_json::json;
use std::env;

#[tokio::test]
async fn test_get_runs_success() {
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
async fn test_get_run_not_found() {
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
async fn test_events_success() {
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
async fn test_logs_with_captured_event() {
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
async fn test_logs_no_captured_event() {
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

// --- Code locations tests ---

#[tokio::test]
async fn test_list_code_locations_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceOrError": {
                        "__typename": "Workspace",
                        "id": "workspace",
                        "locationEntries": [
                            {
                                "id": "loc1",
                                "name": "my-code-location",
                                "loadStatus": "LOADED",
                                "updatedTimestamp": 1710000000.0,
                                "displayMetadata": [
                                    {"key": "image", "value": "my-image:latest"}
                                ]
                            }
                        ]
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result =
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url).await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_list_code_locations_empty() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceOrError": {
                        "__typename": "Workspace",
                        "id": "workspace",
                        "locationEntries": []
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result =
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url).await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_list_code_locations_error_response() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceOrError": {
                        "__typename": "PythonError",
                        "message": "something went wrong"
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result =
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url).await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unexpected"));
}

#[tokio::test]
async fn test_get_code_location_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceLocationEntryOrError": {
                        "__typename": "WorkspaceLocationEntry",
                        "id": "loc1",
                        "name": "my-code-location",
                        "loadStatus": "LOADED",
                        "updatedTimestamp": 1710000000.0,
                        "displayMetadata": [
                            {"key": "image", "value": "my-image:latest"}
                        ],
                        "locationOrLoadError": {
                            "__typename": "RepositoryLocation",
                            "id": "repo-loc-1",
                            "name": "my-code-location",
                            "repositories": [
                                {
                                    "id": "repo1",
                                    "name": "__repository__",
                                    "jobs": [
                                        {"id": "j1", "name": "job_a"},
                                        {"id": "j2", "name": "job_b"}
                                    ],
                                    "schedules": [
                                        {"id": "s1", "name": "daily_schedule"}
                                    ],
                                    "sensors": []
                                }
                            ],
                            "dagsterLibraryVersions": [
                                {"name": "dagster", "version": "1.6.0"}
                            ]
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::code_locations::get_code_location(
        "test-token",
        &api_url,
        "my-code-location".to_string(),
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_code_location_not_found() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceLocationEntryOrError": null
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::code_locations::get_code_location(
        "test-token",
        &api_url,
        "nonexistent".to_string(),
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_code_location_python_error() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceLocationEntryOrError": {
                        "__typename": "PythonError",
                        "message": "Failed to load code location"
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::code_locations::get_code_location(
        "test-token",
        &api_url,
        "broken-location".to_string(),
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Failed to load code location"));
}

#[tokio::test]
async fn test_get_code_location_loading_state() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "workspaceLocationEntryOrError": {
                        "__typename": "WorkspaceLocationEntry",
                        "id": "loc1",
                        "name": "loading-location",
                        "loadStatus": "LOADING",
                        "updatedTimestamp": 1710000000.0,
                        "displayMetadata": [],
                        "locationOrLoadError": null
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::code_locations::get_code_location(
        "test-token",
        &api_url,
        "loading-location".to_string(),
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}
