use clap::Parser;
use opendal::{EntryMode, Operator};

#[derive(Debug, Parser)]
#[command(about = "List objects from an S3-compatible bucket prefix")]
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
    max_keys: i32,
    #[arg(long, default_value_t = true)]
    tls: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let scheme = if args.tls { "https" } else { "http" };
    let endpoint = if args.endpoint.starts_with("http://") || args.endpoint.starts_with("https://")
    {
        args.endpoint.trim_end_matches('/').to_string()
    } else {
        format!("{scheme}://{}", args.endpoint.trim_end_matches('/'))
    };

    let mut options = vec![
        ("bucket".to_string(), args.bucket),
        ("region".to_string(), args.region),
        ("endpoint".to_string(), endpoint),
        ("access_key_id".to_string(), args.access_key),
        ("secret_access_key".to_string(), args.secret_key),
    ];

    if !args.path_style {
        options.push(("enable_virtual_host_style".to_string(), "true".to_string()));
    }

    opendal::init_default_registry();
    let operator = Operator::via_iter("s3", options)?;
    let prefix = args.prefix.trim_start_matches('/').to_string();
    let objects = operator
        .list_with(&prefix)
        .limit(args.max_keys.max(1) as usize)
        .await?;

    println!("count={}", objects.len());
    for object in objects {
        if object.metadata().mode() == EntryMode::DIR {
            continue;
        }
        println!("{}\t{}", object.metadata().content_length(), object.path());
    }

    Ok(())
}
