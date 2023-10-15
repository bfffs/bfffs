use std::sync::{Arc, Mutex};

use divbuf::DivBufShared;
use futures::{
    FutureExt,
    TryFutureExt,
    TryStreamExt,
    stream::FuturesUnordered
};

use bfffs_core::{
    BYTES_PER_LBA,
    Error,
    Uuid,
    TxgT,
    cache::Cache,
    dml::{Compression, DML},
    ddml::*,
};

use crate::{PoolBuilder, test_scale};

mod fault {
    use super::*;

    async fn do_test(ddml: DDML, uuid: Uuid) {
        let ddml = Arc::new(ddml);
        let buf = vec![0; BYTES_PER_LBA];

        let txg = TxgT::from(0);

        let ddml2 = ddml.clone();
        let futs = FuturesUnordered::new();
        for _i in 0..16 {
            let ddml3 = ddml.clone();
            let buf2 = buf.clone();
            let jh = tokio::spawn(async move {
                for _i in 0..3 {
                    let dbs = DivBufShared::from(buf2.clone());
                    ddml3.put(dbs, Compression::None, txg).await.unwrap();
                    eprint!("_");
                }
            }).map_err(|_| Error::EDOOFUS)
            .boxed();
            futs.push(jh);
        }
        let jh = tokio::spawn(async move {
            ddml2.fault(uuid).await.unwrap();
            eprint!("X");
        }).map_err(|_| Error::EDOOFUS)
        .boxed();
        futs.push(jh);
        futs.try_collect::<Vec<_>>().await.unwrap();
        eprintln!();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mirror() {
        for _i in 0..unsafe{test_scale().floor().to_int_unchecked()} {
            let ph = PoolBuilder::new()
                .name("torture_ddml_fault_mirror")
                .disks(2)
                .mirror_size(2)
                .build();
            let uuid = ph.pool.status().clusters[0].mirrors[0].leaves[1].uuid;
            let cache = Cache::with_capacity(1_000_000_000);
            let ddml = DDML::new(ph.pool, Arc::new(Mutex::new(cache)));
            do_test(ddml, uuid).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn raid() {
        for _i in 0..unsafe{test_scale().floor().to_int_unchecked()} {
            let ph = PoolBuilder::new()
                .name("torture_ddml_fault_mirror")
                .disks(3)
                .stripe_size(3)
                .redundancy_level(1)
                .build();
            let uuid = ph.pool.status().clusters[0].mirrors[1].leaves[0].uuid;
            let cache = Cache::with_capacity(1_000_000_000);
            let ddml = DDML::new(ph.pool, Arc::new(Mutex::new(cache)));
            do_test(ddml, uuid).await;
        }
    }
}
