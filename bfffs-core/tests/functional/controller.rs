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
use futures::TryStreamExt;
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
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.new_fs(POOLNAME).await.unwrap();
    }

    /// Try to create a file system that already exists
    #[rstest]
    #[tokio::test]
    async fn eexist(harness: Harness) {
        let fsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&fsname).await.unwrap();
        assert_eq!(
            harness.0.create_fs(&fsname).await.unwrap_err(),
            Error::EEXIST
        );
    }

    /// Create a root file system and a child
    #[rstest]
    #[tokio::test]
    async fn child(harness: Harness) {
        let fsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&fsname).await.unwrap();
        harness.0.new_fs(&fsname).await.unwrap();
    }

    /// Three levels of file system
    #[rstest]
    #[tokio::test]
    async fn grandchild(harness: Harness) {
        let cname = format!("{}/child", POOLNAME);
        let gcname = format!("{}/child/grandchild", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&cname).await.unwrap();
        harness.0.create_fs(&gcname).await.unwrap();
        harness.0.new_fs(&gcname).await.unwrap();
    }

    /// Missing parent
    #[rstest]
    #[tokio::test]
    /// Missing or wrong pool name
    async fn missing_parent(harness: Harness) {
        let gcname = format!("{}/child/grandchild", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        assert_eq!(
            harness.0.create_fs(&gcname).await.unwrap_err(),
            Error::ENOENT
        );
    }

    /// Missing pool name
    #[rstest]
    #[tokio::test]
    async fn missing_pool_name(harness: Harness) {
        assert_eq!(
            harness.0.create_fs("foo").await.unwrap_err(),
            Error::ENOENT
        )
    }
}

mod get_prop {
    use super::*;
    use rstest_reuse::{apply, template};

    fn get_nondefault_value(propname: PropertyName) -> Property {
        match propname {
            PropertyName::Atime => Property::Atime(false),
            PropertyName::BaseMountpoint =>
                Property::BaseMountpoint("/xxx".to_owned()),
            PropertyName::Mountpoint => Property::Mountpoint("/xxx".to_owned()),
            PropertyName::Name => unimplemented!(),
            PropertyName::RecordSize => Property::RecordSize(15),
        }
    }

    // Try to lookup a property for a dataset that does not exist
    #[rstest]
    #[tokio::test]
    async fn enoent(harness: Harness) {
        let dsname = String::from("TestPool/foo");
        assert_eq!(
            Err(Error::ENOENT),
            harness.0.get_prop(dsname, PropertyName::Atime).await
        );
    }

    #[template]
    #[rstest(propname,
        case(PropertyName::Atime),
        case(PropertyName::RecordSize),
        case(PropertyName::Mountpoint)
    )]
    fn all_props(#[case] propname: PropertyName) {}

    #[template]
    #[rstest(propname,
        case(PropertyName::Atime),
        case(PropertyName::RecordSize)
    )]
    fn inheritable_props(#[case] propname: PropertyName) {}

    async fn test(
        harness: Harness,
        source: PropertySource,
        mounted: bool,
        propname: PropertyName)
    {
        let dsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&dsname).await.unwrap();
        let expected = if PropertySource::Default == source {
            Property::default_value(propname)
        } else {
            get_nondefault_value(propname)
        };
        if source == PropertySource::Default {
            // do nothing
        } else if source == PropertySource::LOCAL {
            harness.0.set_prop(&dsname, expected.clone()).await.unwrap();
        } else if source == PropertySource::FROM_PARENT {
                harness.0.set_prop(POOLNAME, expected.clone()).await.unwrap();
        } else {
            unimplemented!();
        }
        let _fs = if mounted {
            Some(harness.0.new_fs(&dsname).await)
        } else {
            None
        };
        assert_eq!(
            (expected, source),
            harness.0.get_prop(dsname, propname).await.unwrap()
        );
    }

    #[apply(inheritable_props)]
    #[tokio::test]
    async fn default_mounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::Default, true, propname).await
    }

    #[apply(inheritable_props)]
    #[tokio::test]
    async fn default_unmounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::Default, false, propname).await
    }

    #[apply(inheritable_props)]
    #[tokio::test]
    async fn inherited_mounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::FROM_PARENT, true, propname).await
    }

    #[apply(inheritable_props)]
    #[tokio::test]
    async fn inherited_unmounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::FROM_PARENT, false, propname).await
    }

    #[apply(all_props)]
    #[tokio::test]
    async fn local_mounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::LOCAL, true, propname).await
    }

    #[apply(all_props)]
    #[tokio::test]
    async fn local_unmounted(harness: Harness, propname: PropertyName) {
        test(harness, PropertySource::LOCAL, false, propname).await
    }

    mod mountpoint {
        use super::*;

        async fn test(
            harness: Harness,
            source: PropertySource,
            mounted: bool)
        {
            let grandparentname = format!("{}/grandparent", POOLNAME);
            let parentname = format!("{}/grandparent/parent", POOLNAME);
            let childname = format!("{}/grandparent/parent/child", POOLNAME);
            harness.0.create_fs(POOLNAME).await.unwrap();
            harness.0.create_fs(&grandparentname).await.unwrap();
            harness.0.create_fs(&parentname).await.unwrap();
            harness.0.create_fs(&childname).await.unwrap();
            let expected = if source == PropertySource::Default {
                Property::mountpoint(format!("/{}/grandparent/parent/child",
                    POOLNAME))
            } else if source == PropertySource::FROM_PARENT {
                harness.0.set_prop(&parentname, Property::mountpoint("/xxx"))
                    .await
                    .unwrap();
                Property::mountpoint("/xxx/child")
            } else if source == PropertySource::FROM_GRANDPARENT {
                harness.0.set_prop(&grandparentname,
                                   Property::mountpoint("/xxx"))
                    .await
                    .unwrap();
                Property::mountpoint("/xxx/parent/child")
            } else {
                unimplemented!();
            };
            let _fs = if mounted {
                Some(harness.0.new_fs(&childname).await)
            } else {
                None
            };
            assert_eq!(
                (expected, source),
                harness.0.get_prop(childname, PropertyName::Mountpoint).await
                    .unwrap()
            );
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn default(harness: Harness, #[case] mounted: bool) {
            test(harness, PropertySource::Default, mounted).await
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn inherited_from_parent(harness: Harness, #[case] mounted: bool)
        {
            test(harness, PropertySource::FROM_PARENT, mounted).await
        }

        #[rstest]
        #[case(false)]
        #[case(true)]
        #[tokio::test]
        async fn inherited_from_grandparent(
            harness: Harness,
            #[case] mounted: bool)
        {
            test(harness, PropertySource::FROM_GRANDPARENT, mounted).await
        }

        /// Get the name pseudoproperty
        #[rstest]
        #[tokio::test]
        async fn name(harness: Harness) {
            let childname = format!("{}/child", POOLNAME);
            harness.0.create_fs(POOLNAME).await.unwrap();
            harness.0.create_fs(&childname).await.unwrap();
            assert_eq!(
                (Property::Name(childname.clone()), PropertySource::None),
                harness.0.get_prop(childname, PropertyName::Name).await.unwrap()
            );
        }
    }
}

mod list_fs {
    use super::*;

    #[rstest]
    #[tokio::test]
    async fn enoent(harness: Harness) {
        assert_eq!(
            Err(Error::ENOENT),
            harness.0.list_fs("TestPool/foo", None)
            .try_collect::<Vec<_>>().await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn enoent_pool(harness: Harness) {
        assert_eq!(
            Err(Error::ENOENT),
            harness.0.list_fs("NoExistPool", None).try_collect::<Vec<_>>().await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn enoent_root_filesystem(harness: Harness) {
        assert_eq!(
            Err(Error::ENOENT),
            harness.0.list_fs(POOLNAME, None).try_collect::<Vec<_>>().await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn no_children(harness: Harness) {
        harness.0.create_fs(POOLNAME).await.unwrap();
        let datasets = harness.0.list_fs(POOLNAME, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, datasets.len());
        assert_eq!(POOLNAME, datasets[0].name);
    }

    #[rstest]
    #[tokio::test]
    async fn one_child(harness: Harness) {
        let dsname = format!("{}/child", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&dsname).await.unwrap();
        let datasets = harness.0.list_fs(POOLNAME, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(2, datasets.len());
        // The order of results is determined by a hash function and is
        // reproducible but not meaningful
        assert_eq!(POOLNAME, datasets[0].name);
        assert_eq!(dsname, datasets[1].name);
    }

    #[rstest]
    #[tokio::test]
    async fn two_children(harness: Harness) {
        let dsname1 = format!("{}/child", POOLNAME);
        let dsname2 = format!("{}/other_child", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&dsname1).await.unwrap();
        harness.0.create_fs(&dsname2).await.unwrap();
        let datasets1 = harness.0.list_fs(POOLNAME, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(3, datasets1.len());
        // The order of results is determined by a hash function and is
        // reproducible but not meaningful
        assert_eq!(POOLNAME, datasets1[0].name);
        assert_eq!(dsname1, datasets1[1].name);
        assert_eq!(dsname2, datasets1[2].name);

        // Now read results again, but providing an offset to skip the parent
        let datasets2 = harness.0.list_fs(POOLNAME, Some(datasets1[0].offs))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(2, datasets2.len());
        assert_eq!(dsname1, datasets2[0].name);
        assert_eq!(dsname2, datasets2[1].name);

        // And again, but skipping the first child
        let datasets3 = harness.0.list_fs(POOLNAME, Some(datasets2[0].offs))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, datasets3.len());
        assert_eq!(dsname2, datasets3[0].name);
    }

    #[rstest]
    #[tokio::test]
    async fn one_grandchild(harness: Harness) {
        let childname = format!("{}/child", POOLNAME);
        let grandchildname = format!("{}/child/grandchild", POOLNAME);
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.create_fs(&childname).await.unwrap();
        harness.0.create_fs(&grandchildname).await.unwrap();
        let l1datasets = harness.0.list_fs(POOLNAME, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(2, l1datasets.len());
        // The order of results is determined by a hash function and is
        // reproducible but not meaningful
        assert_eq!(POOLNAME, l1datasets[0].name);
        assert_eq!(childname, l1datasets[1].name);
        let l2datasets = harness.0.list_fs(&childname, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(2, l2datasets.len());
        assert_eq!(childname, l2datasets[0].name);
        assert_eq!(grandchildname, l2datasets[1].name);
    }
}

mod set_prop {
    use super::*;

    /// Try to set a property on a nonexistent dataset
    #[rstest]
    #[tokio::test]
    async fn enoent(harness: Harness) {
        assert_eq!(
            Err(Error::ENOENT),
            harness.0.set_prop("TestPool/foo", Property::Atime(false)).await
        );
    }

    #[rstest]
    #[tokio::test]
    async fn mounted(harness: Harness) {
        harness.0.create_fs(POOLNAME).await.unwrap();
        let _fs = harness.0.new_fs(POOLNAME).await.unwrap();
        harness.0.set_prop(POOLNAME, Property::Atime(false)).await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    #[should_panic(expected = "Immutable property")]
    async fn set_prop(harness: Harness) {
        harness.0.create_fs(POOLNAME).await.unwrap();
        let _fs = harness.0.new_fs(POOLNAME).await.unwrap();
        harness.0.set_prop(POOLNAME, Property::Name(String::from("xxx")))
            .await.unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn unmounted(harness: Harness) {
        harness.0.create_fs(POOLNAME).await.unwrap();
        harness.0.set_prop(POOLNAME, Property::Atime(false)).await.unwrap();
    }

    mod mountpoint {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn relative(harness: Harness) {
            harness.0.create_fs(POOLNAME).await.unwrap();
            let prop = Property::mountpoint("relative_path");
            let e = harness.0.set_prop(POOLNAME, prop).await;
            assert_eq!(Err(Error::EINVAL), e);
        }
    }
}
