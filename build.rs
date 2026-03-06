fn main() {
    // Only register schema if it exists
    if std::path::Path::new("schemas/dagster.graphql").exists() {
        cynic_codegen::register_schema("dagster")
            .from_sdl_file("schemas/dagster.graphql")
            .unwrap()
            .as_default()
            .unwrap();
    } else {
        println!("cargo:warning=Schema file not found. Run 'dagster-cli schema download' first.");
    }
}
