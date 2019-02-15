extern crate futures;
extern crate rusoto_core;
extern crate rusoto_athena;
extern crate env_logger;

use futures::future::Future;
use rusoto_core::{Region, RusotoFuture};
use rusoto_athena::*;
use std::{thread, time};
use uuid::Uuid;

// fn get_query_output(client: AthenaClient, query_id: String) -> () {
//     let query_result_input = GetQueryResultsInput {
//         max_results: None,
//         next_token: None,
//         query_execution_id: query_id
//     };

//     match client.get_query_execution(query_result_input).sync() {
//         Err(error) => println!("Error: {:?}", error),
//         Ok(output) => {
//             output.result_set_metadata.to_iter().fold((), |metadata| println!("metadata: {:?}", metadata));
//             match output.rows {
//                 None => println!("No rows."),
//                 Some(rows) => for row in rows.to_iter() {
//                     println!("{}", row)
//                 }
//             }
//         }
//     }
// }

enum QueryExecutionError {
    NoStateField,
    QueryCancelled { query_execution_id: String, database: String },
    QueryFailed { query_execution_id: String, database: String }
}

fn submit_query(client: AthenaClient, query_string: String) -> RusotoFuture<StartQueryExecutionOutput, StartQueryExecutionError> {
    let query_input = StartQueryExecutionInput {
        client_request_token: Some(Uuid::new_v4().to_string()),
        query_execution_context: Some(QueryExecutionContext {
            database: Some("tb".to_string())
        }),
        query_string: query_string,
        result_configuration: ResultConfiguration {
            encryption_configuration: None,
            output_location: "s3://dvm105-tb/results".to_string()
        }
    };

    client.start_query_execution(query_input)
}

fn process_query_execution(output: GetQueryExecutionOutput) -> RusotoFuture<String, QueryExecutionError> {
    let state: Result<String, QueryExecutionError> = output.query_execution.and_then(|qe| qe.status.and_then(|status| status.state)).ok_or(QueryExecutionError::NoStateField);
    let query_id: String = output.query_execution.and_then(|qe| qe.query_execution_id).unwrap_or("No query id provided".to_string());
    let database: String = output.query_execution.and_then(|qe| qe.query_execution_context.and_then(|ctx| ctx.database)).unwrap_or("No database provided".to_string());

    let stateResult: Result<String, QueryExecutionError> = state.and_then(|state_str| match state_str.as_ref() {
        "FAILED"    => Err(QueryExecutionError::QueryFailed { query_execution_id: query_id, database: database }).into(),
        "CANCELLED" => Err(QueryExecutionError::QueryCancelled { query_execution_id: query_id, database: database }).into(),
        "SUCCEEDED" => Ok(query_id),
        _ => unimplemented!()
    });

    stateResult.into()

    // sleep and retry
    // thread::sleep(time::Duration::from_millis(1000));
    // wait_for_completion(client, query_id)
    
}

fn wait_for_completion(client: AthenaClient, query_id: String) -> RusotoFuture<String, QueryExecutionError> {   
    let query_execution_input = GetQueryExecutionInput {
        query_execution_id: query_id
    };
    
    client.get_query_execution(query_execution_input).and_then(|result| process_query_execution(result))
}

fn main() {
    let _ = env_logger::try_init(); // This initializes the `env_logger`
    let client = AthenaClient::new(Region::UsEast2);
    let query_exectution_output = submit_query(client, "SELECT * FROM dvm105_tb LIMIT 3".to_string());

    // match client.start_query_execution(query_input).sync() {
    //     Ok(output) => {
    //         match output.query_execution_id {
    //             Some(query_id) => println!("query running. id: {}", query_id),
    //             None => println!("query running. no id found"),
    //         } 
    //     },
    //     Err(error) => {
    //         println!("Error: {:?}", error);
    //     },
    // }

    // let query_input = ListQueryExecutionsInput {
    //     max_results: None,
    //     next_token: None
    // };

    // match client.list_query_executions(query_input).sync() {
    //     Ok(output) => {
    //         match output.query_execution_ids {
    //             None => println!("No query ids"),
    //             Some(ids) => println!("ids: {:?}", ids)
    //         } 
    //     },
    //     Err(error) => println!("error: {}", error)
    // }


}