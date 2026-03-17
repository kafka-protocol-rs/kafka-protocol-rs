use failure::Error;
use git2::{Oid, Repository};
use std::fs;
use std::path::Path;
use std::process::Command;

pub mod customizations;
pub mod generate_messages;

fn main() -> Result<(), Error> {
    let mut dir = std::fs::canonicalize(std::file!().rsplit_once('/').unwrap().0)?;
    dir.push("../../src/messages");
    let output_path = std::fs::canonicalize(dir)?;
    let messages_module_dir = output_path.to_str().unwrap();

    // Download messages from head of Kafka repo
    let kafka_repo = Path::new("kafka_repo");
    let repo = if kafka_repo.exists() {
        println!("Fetching latest kafka repo");
        let repo = Repository::open(kafka_repo)?;
        repo.find_remote("origin")
            .unwrap()
            .fetch(&["trunk"], None, None)
            .unwrap();
        repo
    } else {
        println!("Cloning kafka repo");
        git2::build::RepoBuilder::new()
            .fetch_options(git2::FetchOptions::new())
            .with_checkout(git2::build::CheckoutBuilder::new())
            .clone("https://github.com/apache/kafka.git", kafka_repo)?
    };

    // Checkout the release commit
    // https://github.com/apache/kafka/releases/tag/4.1.0
    // checking out a tag with git2 is annoying -- we pin to the tag's commit sha instead
    let release_commit = "13f70256db3c994c590e5d262a7cc50b9e973204";
    println!("Checking out release {}", release_commit);
    let oid = Oid::from_str(release_commit).unwrap();
    let commit = repo
        .find_commit(oid)
        .expect("Could not find release commit!")
        .into_object();
    repo.checkout_tree(&commit, None).unwrap();
    repo.set_head_detached(commit.id()).unwrap();

    // Clear output directory
    for file in fs::read_dir(messages_module_dir)? {
        let file = file?;
        if file.file_type()?.is_file() {
            let path = file.path();
            if path.extension() == Some("rs".as_ref()) {
                fs::remove_file(path)?;
            }
        }
    }

    // Find input files
    let mut input_file_paths = Vec::new();
    for file in fs::read_dir(kafka_repo.join("clients/src/main/resources/common/message"))? {
        let file = file?;
        if file.file_type()?.is_file() {
            let path = file.path();
            if path.extension() == Some("json".as_ref()) {
                input_file_paths.push(path);
            }
        }
    }

    generate_messages::run(messages_module_dir, input_file_paths)?;

    println!("Applying post-generation customizations...");
    customizations::apply(messages_module_dir).expect("Failed to apply customizations");

    println!("Running cargo fmt...");
    let mut process = Command::new("cargo")
        .args(vec!["fmt"])
        .spawn()
        .expect("cargo fmt failed");

    process.wait().expect("cargo fmt failed");

    Ok(())
}
