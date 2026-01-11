fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create output directory for generated code
    let out_dir = std::path::Path::new("src/protocol/generated");
    std::fs::create_dir_all(out_dir)?;
    
    // Proto compilation requires protoc to be installed
    // To enable, install protoc and uncomment below:
    let proto_path = std::path::Path::new("proto/dtrainer.proto");
    // Generate gRPC code
    // tonic_build::configure()
    //     .build_server(true)
    //     .build_client(true)
    //     .out_dir("src/protocol/generated")
    //     .compile(
    //         &["proto/dtrainer.proto"],
    //         &["proto"]
    //     )?;
    
    Ok(())
}
