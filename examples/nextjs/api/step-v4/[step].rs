use serde_json::{json, Value};
use http::Method;
use std::convert::Infallible;
use vercel_runtime::{run, Body, Error, Request, Response, StatusCode, wait_until};

// Import your service functions (assumed available in Rust)
use crate::db::mongo::store_execution_data_v2;
use crate::services::graph::construct_nodes_graph;
use crate::services::node_executors::{execute_single_node, execute_nodes_group};
use crate::services::workflow::{get_workflow_state, set_workflow_node_state};

#[tokio::main]
async fn main() -> Result<(), Error> {
    run(handler).await
}

pub async fn handler(req: Request) -> Result<Response<Body>, Error> {
    // Handle CORS preflight
    if req.method() == Method::OPTIONS {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type")
            .body(Body::Empty)?);
    }

    // Always include CORS headers on actual responses
    let mut builder = Response::builder()
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type");

    // --- Parse step index from the path `/api/step-v3/{step}` ---
    let step_index = req
        .uri()
        .path()
        .rsplit('/')
        .next()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    // --- Parse JSON body ---
    let body: Value = req.json().await?;
    let nodes = body
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::from("nodes array is missing"))?
        .clone();
    let edges = body
        .get("edges")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let workflow_id = body
        .get("workflow_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let user_id = body
        .get("user_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let trigger_output = body.get("trigger_output").cloned().unwrap_or_else(|| json!({}));
    let webhook_body = body.get("webhook_body").cloned().unwrap_or_else(|| json!({}));

    // --- Load or initialize previous results ---
    let mut existing_results: Vec<Value> = get_workflow_state(&trigger_output)
        .await
        .unwrap_or_else(|_| Vec::new());

    // --- Build the node graph ---
    let graph = construct_nodes_graph(nodes, edges);
    let node_or_group = &graph[step_index];

    // --- Execute this step ---
    let result: Value = if node_or_group.is_group() {
        execute_nodes_group(node_or_group.clone(), trigger_output.clone(), webhook_body.clone())
            .await?
    } else {
        execute_single_node(node_or_group.clone(), trigger_output.clone(), webhook_body.clone())
            .await?
    };

    // --- Streaming branch (SSE) ---
    if result.is_stream() {
        // Create an SSE response
        let mut response = builder
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache, no-transform")
            .header("Connection", "keep-alive")
            .body(Body::Empty)?;

        let mut token_buf = Vec::new();
        let mut stream = result.into_stream().unwrap();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let text = serde_json::to_string(&chunk)?;
            // Send chunk immediately
            response.write(format!("data: {}\n\n", text).as_bytes()).unwrap();

            if let Some(tok) = chunk.get("token").and_then(Value::as_str) {
                token_buf.push(tok.to_string());
            }
        }

        // Build unified completion
        if !token_buf.is_empty() {
            let unified = json!({
                "id": format!("cmpl-{:x}", chrono::Utc::now().timestamp_millis()),
                "object": "chat.completion",
                "created": chrono::Utc::now().timestamp(),
                "model": "stream-reconstructed",
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": token_buf.join("") },
                    "finish_reason": "stop"
                }]
            });
            existing_results.push(unified.clone());

            if let Some(node_id) = node_or_group.id() {
                wait_until(set_workflow_node_state(&trigger_output, node_id, json!({ "data": unified })));
            }
        }

        // Redirect or finish
        if step_index + 1 < graph.len() {
            let next = format!("/api/step-v3/{}", step_index + 1);
            response
                .write(format!("event: redirect\ndata: {}\n\n", next).as_bytes())
                .unwrap();
        } else {
            wait_until(store_execution_data_v2(existing_results.clone(), &workflow_id, &user_id));
        }

        return Ok(response.end()?);
    }

    // --- Non-streaming branch ---
    existing_results.push(result.clone());
    if let Some(node_id) = node_or_group.id() {
        wait_until(set_workflow_node_state(&trigger_output, node_id, json!({ "data": result })));
    }

    if step_index + 1 < graph.len() {
        let location = format!("/api/step-v3/{}", step_index + 1);
        return Ok(builder
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header("Location", location)
            .body(Body::from(json!({ "data": existing_results }).to_string()))?);
    }

    wait_until(store_execution_data_v2(existing_results.clone(), &workflow_id, &user_id));
    Ok(builder
        .status(StatusCode::OK)
        .body(Body::from(json!({ "data": existing_results }).to_string()))?)
}
