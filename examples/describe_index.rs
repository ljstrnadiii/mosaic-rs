#[cfg(feature = "geoparquet")]
fn main() -> anyhow::Result<()> {
    use arrow2::io::parquet::read::{infer_schema, read_metadata};
    use std::fs::File;

    let path = std::env::args()
        .nth(1)
        .expect("usage: describe_index <parquet>");

    let mut file = File::open(&path)?;
    let metadata = read_metadata(&mut file)?;
    let schema = infer_schema(&metadata)?;

    println!("rows: {}", metadata.num_rows);
    println!("row groups: {}", metadata.row_groups.len());
    println!("\nschema:");
    for f in &schema.fields {
        println!("  {}: {:?}", f.name, f.data_type);
    }

    Ok(())
}

#[cfg(not(feature = "geoparquet"))]
fn main() {
    eprintln!("Enable `geoparquet` feature to run this example.");
}
