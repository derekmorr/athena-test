extern crate rusoto_core;
extern crate rusoto_athena;
extern crate env_logger;

use rusoto_core::Region;
use rusoto_athena::*;
use uuid::Uuid;

fn main() {
    let _ = env_logger::try_init(); // This initializes the `env_logger`

    let client = AthenaClient::new(Region::UsEast2);
    
    let request_token = Uuid::new_v4();

    let query_input = StartQueryExecutionInput {
        client_request_token: Some(request_token.to_string()),
        query_execution_context: Some(QueryExecutionContext {
            database: Some("tb".to_string())
        }),
        query_string: "SELECT * FROM dvm105_tb LIMIT 3".to_string(),
        result_configuration: ResultConfiguration {
            encryption_configuration: None,
            output_location: "s3://dvm105-tb/results".to_string()
        }
    };

    match client.start_query_execution(query_input).sync() {
        Ok(output) => {
            match output.query_execution_id {
                Some(query_id) => println!("query running. id: {}", query_id),
                None => println!("query running. no id found"),
            } 
        },
        Err(error) => {
            println!("Error: {:?}", error);
        },
    }

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