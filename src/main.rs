use rdf2hdt::*;

fn main() {
    println!("Hello, world!");
    let res = rdf2hdt::build_hdt(vec!["my.file".to_string()], "test.hdt");
}
