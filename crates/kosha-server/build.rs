// Rebuild when the web console is rebuilt, so the embedded copy is current.
fn main() {
    println!("cargo:rerun-if-changed=../../ui/dist");
}
