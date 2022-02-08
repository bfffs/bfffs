// vim: tw=80
use bfffs_core::{
    Error,
    cache::*,
    controller::Controller,
    database::Database,
    ddml::*,
    idml::*,
    pool::*,
    property::{Property, PropertyName, PropertySource}
};
use rstest::{fixture, rstest};
use std::{
    fs,
    sync::{Arc, Mutex}
};
use tempfile::Builder;


const POOLNAME: &str = "TestPool";

type Harness = (Controller,);

#[fixture]
fn harness() -> Harness {
    let len = 1 << 26;  // 64 MB
    let tempdir = Builder::new()
        .prefix("test_controller")
        .tempdir()
        .unwrap();
    let filename = tempdir.path().join("vdev");
    {
        let file = fs::File::create(&filename).unwrap();
        file.set_len(len).unwrap();
    }
    let cache = Arc::new(Mutex::new(Cache::with_capacity(1_000_000)));
    let cluster = Pool::create_cluster(None, 1, None, 0, &[filename]);
    let pool = Pool::create(String::from(POOLNAME), vec![cluster]);
    let ddml = Arc::new(DDML::new(pool, cache.clone()));
    let idml = IDML::create(ddml, cache);
    let db = Database::create(Arc::new(idml));
    (Controller::new(db),)
}

mod create_fs {
    use super::*;

    /// Create the root file system and open it.
    #[rstest]
    #[tokio::test]
    async fn root_fs(harness: Harness) {
        harness.0.create_fs(POOLNAME, vec![]).await.unwrap();
        harness.0.new_fs(POOLNAME).await.unwrap();
    }

    /// Try to create a file system that already exists
    #[rstest]
    #[tokio::test]
    async fn eexist(harness: Harness) {
        let fsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME, vec![]).await.unwrap();
        harness.0.create_fs(&fsname, vec![]).await.unwrap();
        assert_eq!(
            harness.0.create_fs(&fsname, vec![]).await.unwrap_err(),
            Error::EEXIST
        );
    }

    /// Create a root file system and a child
    #[rstest]
    #[tokio::test]
    async fn child(harness: Harness) {
        let fsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME, vec![]).await.unwrap();
        harness.0.create_fs(&fsname, vec![]).await.unwrap();
        harness.0.new_fs(&fsname).await.unwrap();
    }

    /// Three levels of file system
    #[rstest]
    #[tokio::test]
    async fn grandchild(harness: Harness) {
        let cname = format!("{}/child", POOLNAME);
        let gcname = format!("{}/child/grandchild", POOLNAME);
        harness.0.create_fs(POOLNAME, vec![]).await.unwrap();
        harness.0.create_fs(&cname, vec![]).await.unwrap();
        harness.0.create_fs(&gcname, vec![]).await.unwrap();
        harness.0.new_fs(&gcname).await.unwrap();
    }

    /// Missing parent
    #[rstest]
    #[tokio::test]
    /// Missing or wrong pool name
    async fn missing_parent(harness: Harness) {
        let gcname = format!("{}/child/grandchild", POOLNAME);
        harness.0.create_fs(POOLNAME, vec![]).await.unwrap();
        assert_eq!(
            harness.0.create_fs(&gcname, vec![]).await.unwrap_err(),
            Error::ENOENT
        );
    }

    /// Missing pool name
    #[rstest]
    #[tokio::test]
    async fn missing_pool_name(harness: Harness) {
        assert_eq!(
            harness.0.create_fs("foo", vec![]).await.unwrap_err(),
            Error::ENOENT
        )
    }

    #[rstest]
    #[tokio::test]
    async fn with_props(harness: Harness) {
        let propname = PropertyName::Atime;
        let props = vec![Property::Atime(false)];
        harness.0.create_fs(POOLNAME, props).await.unwrap();
        let (value, source) = harness.0.get_prop(POOLNAME, propname).await
            .unwrap();
        assert_eq!(Property::Atime(false), value);
        assert_eq!(PropertySource::Local, source);
    }
}
