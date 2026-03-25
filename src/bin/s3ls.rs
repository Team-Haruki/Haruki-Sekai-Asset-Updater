use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::Client;
use clap::Parser;

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

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(args.region.clone()))
        .load()
        .await;

    let credentials =
        aws_sdk_s3::config::Credentials::new(args.access_key, args.secret_key, None, None, "s3ls");

    let scheme = if args.tls { "https" } else { "http" };
    let endpoint_url = format!("{scheme}://{}", args.endpoint);
    let client = Client::from_conf(
        Builder::from(&shared_config)
            .endpoint_url(endpoint_url)
            .force_path_style(args.path_style)
            .credentials_provider(credentials)
            .build(),
    );

    let response = client
        .list_objects_v2()
        .bucket(args.bucket)
        .prefix(args.prefix)
        .max_keys(args.max_keys)
        .send()
        .await?;

    let objects = response.contents();
    println!("count={}", objects.len());
    for object in objects {
        let key = object.key().unwrap_or_default();
        let size = object.size().unwrap_or_default();
        println!("{size}\t{key}");
    }

    Ok(())
}
