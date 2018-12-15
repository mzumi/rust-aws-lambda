#[macro_use]
extern crate lambda_runtime as lambda;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate simple_logger;

use lambda::error::HandlerError;

use std::error::Error;

#[derive(Deserialize, Clone)]
struct CustomEvent {
    x: i64,
    y: i64,
}

#[derive(Serialize, Clone)]
struct CustomOutput {
    result: i64,
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Info)?;
    lambda!(my_handler);

    Ok(())
}

fn my_handler(e: CustomEvent, c: lambda::Context) -> Result<CustomOutput, HandlerError> {
    println!("{}", c.aws_request_id);

    println!("x: {}, y: {}", e.x, e.y);

    if e.y == 0 {
        error!("Invalid denominator in request {}", c.aws_request_id);
        return Err(c.new_error("denominator is 0"));
    }

    Ok(CustomOutput {
        result: (e.x / e.y),
    })
}
