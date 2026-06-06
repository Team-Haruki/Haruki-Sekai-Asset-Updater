use std::collections::BTreeMap;

use clap::Parser;
use opendal::Operator;

#[derive(Debug, Parser)]
#[command(about = "List objects from an S3-compatible bucket prefix via OpenDAL")]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long)]
    region: String,
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    prefix: String,
    #[arg(long)]
    access_key: String,
    #[arg(long)]
    secret_key: String,
    #[arg(long, default_value_t = false)]
    path_style: bool,
    #[arg(long, default_value_t = 100)]
    max_keys: usize,
    #[arg(long, default_value_t = true)]
    tls: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let scheme = if args.tls { "https" } else { "http" };
    let endpoint = if args.endpoint.starts_with("http://") || args.endpoint.starts_with("https://")
    {
        args.endpoint.clone()
    } else {
        format!("{scheme}://{}", args.endpoint)
    };

    opendal::init_default_registry();
    let mut options = BTreeMap::from([
        ("bucket".to_string(), args.bucket),
        ("endpoint".to_string(), endpoint),
        ("region".to_string(), args.region),
        ("access_key_id".to_string(), args.access_key),
        ("secret_access_key".to_string(), args.secret_key),
    ]);
    if !args.path_style {
        options.insert("enable_virtual_host_style".to_string(), "true".to_string());
    }

    let operator = Operator::via_iter("s3", options)?;
    let entries = operator.list_with(&args.prefix).recursive(true).await?;

    println!("count={}", entries.len().min(args.max_keys));
    for entry in entries.into_iter().take(args.max_keys) {
        let meta = entry.metadata();
        println!("{}\t{}", meta.content_length(), entry.path());
    }

    Ok(())
}
