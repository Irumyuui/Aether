fn main() {
    let conf = aether::config::DBConfig::default();
    let db = aether::database::DBEngine::new(conf).unwrap();

    db.put("key1".into(), "114514".into()).unwrap();
    db.put("key2".into(), "1919810".into()).unwrap();

    println!("key1 = {:?}", db.get("key1".into()));
    println!("key2 = {:?}", db.get("key2".into()));
}
