use rbs::value::map::ValueMap;

fn main() {
    println!("Hello, world!");
    let mut m = ValueMap::new();
    m.insert("1".into(), 1.into());
    m.insert("2".into(), 2.into());
    assert_eq!(m.to_string(), r#"{"1":1,"2":2}"#);
}
