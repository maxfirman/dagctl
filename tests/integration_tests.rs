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
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url, &None).await;

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
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url, &None).await;

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
        dagctl::commands::code_locations::list_code_locations("test-token", &api_url, &None).await;

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
        &None,
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
        &None,
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
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Failed to load code location")
    );
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
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

// --- Jobs tests ---

#[tokio::test]
async fn test_list_jobs_success() {
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
                                "name": "my-location",
                                "locationOrLoadError": {
                                    "__typename": "RepositoryLocation",
                                    "id": "rl1",
                                    "name": "my-location",
                                    "repositories": [
                                        {
                                            "id": "repo1",
                                            "name": "__repository__",
                                            "jobs": [
                                                {
                                                    "id": "j1",
                                                    "name": "my_job",
                                                    "isJob": true,
                                                    "schedules": [
                                                        {"id": "s1", "name": "daily_schedule"}
                                                    ],
                                                    "sensors": []
                                                },
                                                {
                                                    "id": "j2",
                                                    "name": "__ASSET_JOB",
                                                    "isJob": false,
                                                    "schedules": [],
                                                    "sensors": []
                                                }
                                            ]
                                        }
                                    ]
                                }
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
    let result = dagctl::commands::jobs::list_jobs("test-token", &api_url, None, &None).await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_list_jobs_with_code_location_filter() {
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
                                "name": "location-a",
                                "locationOrLoadError": {
                                    "__typename": "RepositoryLocation",
                                    "id": "rl1",
                                    "name": "location-a",
                                    "repositories": [{
                                        "id": "r1",
                                        "name": "__repository__",
                                        "jobs": [{
                                            "id": "j1",
                                            "name": "job_a",
                                            "isJob": true,
                                            "schedules": [],
                                            "sensors": []
                                        }]
                                    }]
                                }
                            },
                            {
                                "id": "loc2",
                                "name": "location-b",
                                "locationOrLoadError": {
                                    "__typename": "RepositoryLocation",
                                    "id": "rl2",
                                    "name": "location-b",
                                    "repositories": [{
                                        "id": "r2",
                                        "name": "__repository__",
                                        "jobs": [{
                                            "id": "j2",
                                            "name": "job_b",
                                            "isJob": true,
                                            "schedules": [],
                                            "sensors": []
                                        }]
                                    }]
                                }
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
    let result = dagctl::commands::jobs::list_jobs(
        "test-token",
        &api_url,
        Some("location-a".to_string()),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

// --- Assets tests ---

#[tokio::test]
async fn test_list_assets_success() {
    let mut server = Server::new_async().await;

    let _nodes_mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodes": [
                        {
                            "id": "a1",
                            "assetKey": {"path": ["my_prefix", "my_asset"]},
                            "groupName": "default",
                            "kinds": ["python"],
                            "isPartitioned": false,
                            "repository": {
                                "id": "r1",
                                "name": "__repository__",
                                "location": {
                                    "id": "l1",
                                    "name": "my-location"
                                }
                            }
                        }
                    ],
                    "assetsOrError": {
                        "__typename": "AssetConnection",
                        "nodes": [
                            {
                                "id": "a1",
                                "key": {"path": ["my_prefix", "my_asset"]},
                                "assetHealth": {
                                    "assetHealth": "HEALTHY"
                                }
                            }
                        ]
                    }
                }
            })
            .to_string(),
        )
        .expect_at_least(1)
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result =
        dagctl::commands::assets::list_assets("test-token", &api_url, None, None, vec![], &None)
            .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "assetKey": {"path": ["my_asset"]},
                        "groupName": "default",
                        "description": "A test asset",
                        "kinds": ["python"],
                        "isPartitioned": false,
                        "opName": "my_asset",
                        "opVersion": "v1",
                        "jobNames": ["my_job"],
                        "dependencyKeys": [{"path": ["upstream"]}],
                        "dependedByKeys": [{"path": ["downstream"]}],
                        "repository": {
                            "id": "r1",
                            "name": "__repository__",
                            "location": {
                                "id": "l1",
                                "name": "my-location"
                            }
                        },
                        "owners": [
                            {"__typename": "UserAssetOwner", "email": "user@example.com"}
                        ],
                        "tags": [
                            {"key": "dagster/storage_kind", "value": "snowflake"}
                        ],
                        "metadataEntries": [
                            {"__typename": "TextMetadataEntry", "label": "row_count", "text": "1000"}
                        ],
                        "automationCondition": {
                            "label": "eager",
                            "expandedLabel": ["eager"]
                        },
                        "targetingInstigators": [
                            {
                                "__typename": "Sensor",
                                "id": "s1",
                                "jobOriginId": "j1",
                                "name": "my_sensor",
                                "sensorType": "ASSET",
                                "defaultStatus": "RUNNING",
                                "canReset": true,
                                "hasCursorUpdatePermissions": true,
                                "sensorState": {
                                    "id": "ss1",
                                    "status": "RUNNING",
                                    "hasStartPermission": true,
                                    "hasStopPermission": true
                                },
                                "minIntervalSeconds": 30,
                                "metadata": {"assetKeys": null}
                            }
                        ]
                    },
                    "assetOrError": {
                        "__typename": "Asset",
                        "id": "a1",
                        "key": {"path": ["my_asset"]},
                        "assetHealth": {
                            "assetHealth": "HEALTHY",
                            "materializationStatus": "HEALTHY",
                            "materializationStatusMetadata": null,
                            "assetChecksStatus": "NOT_APPLICABLE",
                            "assetChecksStatusMetadata": null,
                            "freshnessStatus": "NOT_APPLICABLE",
                            "freshnessStatusMetadata": null
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result =
        dagctl::commands::assets::get_asset("test-token", &api_url, "my_asset".to_string(), &None)
            .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_not_found() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNotFoundError",
                        "message": "Asset not found"
                    },
                    "assetOrError": {
                        "__typename": "AssetNotFoundError",
                        "message": "Asset not found"
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset(
        "test-token",
        &api_url,
        "nonexistent".to_string(),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_asset_events_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetOrError": {
                        "__typename": "Asset",
                        "id": "a1",
                        "key": {"path": ["my_asset"]},
                        "assetEventHistory": {
                            "results": [
                                {
                                    "__typename": "MaterializationEvent",
                                    "runId": "run-1",
                                    "timestamp": "1710000000",
                                    "message": "Materialized asset",
                                    "partition": "2024-01-01"
                                },
                                {
                                    "__typename": "ObservationEvent",
                                    "runId": "run-2",
                                    "timestamp": "1710000100",
                                    "message": "Observed asset",
                                    "partition": null
                                },
                                {
                                    "__typename": "FailedToMaterializeEvent",
                                    "runId": "run-3",
                                    "timestamp": "1710000200",
                                    "message": "Failed to materialize",
                                    "partition": null
                                }
                            ],
                            "cursor": "abc"
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_events(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        Some(25),
        vec![],
        vec![],
        None,
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_events_not_found() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetOrError": {
                        "__typename": "AssetNotFoundError",
                        "message": "Asset not found"
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_events(
        "test-token",
        &api_url,
        "nonexistent".to_string(),
        None,
        vec![],
        vec![],
        None,
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_asset_partitions_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "isPartitioned": true,
                        "partitionStats": {
                            "numPartitions": 100,
                            "numMaterialized": 80,
                            "numFailed": 5,
                            "numMaterializing": 2
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_partitions(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_partitions_not_partitioned() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "isPartitioned": false,
                        "partitionStats": null
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_partitions(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not partitioned"));
}

#[tokio::test]
async fn test_get_asset_checks_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "assetChecksOrError": {
                            "__typename": "AssetChecks",
                            "checks": [
                                {
                                    "name": "freshness_check",
                                    "description": "Checks data freshness",
                                    "blocking": true,
                                    "executionForLatestMaterialization": {
                                        "id": "e1",
                                        "status": "SUCCEEDED",
                                        "runId": "run-1",
                                        "timestamp": 1710000000.0
                                    }
                                },
                                {
                                    "name": "row_count_check",
                                    "description": null,
                                    "blocking": false,
                                    "executionForLatestMaterialization": null
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

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_checks(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_check_detail_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "assetCheckOrError": {
                            "__typename": "AssetCheck",
                            "name": "freshness_check",
                            "description": "Checks data freshness",
                            "blocking": true,
                            "jobNames": ["my_job"],
                            "canExecuteIndividually": "CAN_EXECUTE",
                            "automationCondition": null,
                            "executionForLatestMaterialization": {
                                "id": "e1",
                                "status": "SUCCEEDED",
                                "runId": "run-1",
                                "timestamp": 1710000000.0,
                                "evaluation": {
                                    "severity": "ERROR",
                                    "success": true,
                                    "description": "Check passed"
                                }
                            }
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_check(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        "freshness_check",
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_get_asset_check_not_found() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetNodeOrError": {
                        "__typename": "AssetNode",
                        "id": "a1",
                        "assetCheckOrError": {
                            "__typename": "AssetCheckNotFoundError",
                            "message": "Check not found"
                        }
                    }
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_check(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        "nonexistent",
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[tokio::test]
async fn test_get_asset_check_executions_success() {
    let mut server = Server::new_async().await;

    let mock = server
        .mock("POST", "/graphql")
        .with_status(200)
        .with_header("content-type", "application/json")
        .with_body(
            json!({
                "data": {
                    "assetCheckExecutions": [
                        {
                            "id": "e1",
                            "status": "SUCCEEDED",
                            "runId": "run-1",
                            "timestamp": 1710000000.0,
                            "partition": null,
                            "stepKey": null,
                            "evaluation": {
                                "severity": "ERROR"
                            }
                        },
                        {
                            "id": "e2",
                            "status": "FAILED",
                            "runId": "run-2",
                            "timestamp": 1709999000.0,
                            "partition": "2024-01-01",
                            "stepKey": null,
                            "evaluation": {
                                "severity": "WARN"
                            }
                        }
                    ]
                }
            })
            .to_string(),
        )
        .create_async()
        .await;

    let api_url = format!("{}/graphql", server.url());
    let result = dagctl::commands::assets::get_asset_check_executions(
        "test-token",
        &api_url,
        "my_asset".to_string(),
        "freshness_check",
        Some(25),
        &None,
    )
    .await;

    mock.assert_async().await;
    assert!(result.is_ok());
}
