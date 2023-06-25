use std::{
    cmp::Ordering,
    fmt,
    io::{self, Write},
    mem,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::Arc,
};

use bfffs::{Bfffs, Error, Result};
use bfffs_core::{
    controller::Controller,
    database::{Database, TreeID},
    device_manager::DevManager,
    property::{Property, PropertyName, PropertySource},
};
use clap::{crate_version, ArgAction, Parser};
use futures::{future, TryStreamExt};
use tracing_subscriber::EnvFilter;

mod pool_create_ast;

#[derive(Parser, Clone, Debug)]
/// Consistency check
struct Check {
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks:     Vec<PathBuf>,
}

impl Check {
    // Offline consistency check.  Checks that:
    // * RAID parity is consistent
    // * Checksums match
    // * RIDT and AllocT are exact inverses
    // * RIDT contains no orphan entries not found in the FSTrees
    // * Spacemaps match actual usage
    pub async fn main(self) -> Result<()> {
        let dev_manager = DevManager::default();
        for dev in self.disks.iter() {
            dev_manager.taste(dev).await.unwrap();
        }

        let db = Arc::new(
            dev_manager
                .import_by_name(self.pool_name)
                .await
                .unwrap_or_else(|_e| {
                    eprintln!("Error: pool not found");
                    exit(1);
                }),
        );
        db.check().await.unwrap();
        // TODO: the other checks
        Ok(())
    }
}

#[derive(Parser, Clone, Debug)]
/// Drop all in-memory caches, for testing or benchmark purposes.
struct DropCache {}

impl DropCache {
    async fn main(self, sock: &Path) -> Result<()> {
        let bfffs = Bfffs::new(sock).await.unwrap();
        bfffs.drop_cache().await
    }
}

#[derive(Parser, Clone, Debug)]
/// Dump internal filesystem information
struct Dump {
    /// Dump the Allocation Table
    #[clap(long)]
    alloct:    bool,
    /// Dump the Forest
    #[clap(long)]
    forest:    bool,
    /// Dump the Free Space Map
    #[clap(short, long)]
    fsm:       bool,
    /// Dump the Record Indirection Table
    #[clap(long)]
    ridt:      bool,
    /// Dump the file system tree
    #[clap(short, long)]
    tree:      bool,
    #[clap(required(true))]
    /// Pool name
    pool_name: String,
    #[clap(required(true))]
    disks:     Vec<PathBuf>,
}

impl Dump {
    async fn dump_alloct(self) {
        let db = self.load_db().await;
        db.dump_alloct(&mut io::stdout()).await.unwrap()
    }

    async fn dump_forest(self) {
        let db = self.load_db().await;
        db.dump_forest(&mut io::stdout()).await.unwrap()
    }

    async fn dump_fsm(self) {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk).await.unwrap();
        }
        let uuid = dev_manager
            .importable_pools()
            .iter()
            .find(|(name, _uuid)| *name == self.pool_name)
            .unwrap()
            .1;
        let clusters = dev_manager.import_clusters(uuid).await.unwrap();
        for c in clusters {
            println!("{}", c.dump_fsm());
        }
    }

    async fn dump_ridt(self) {
        let db = self.load_db().await;
        db.dump_ridt(&mut io::stdout()).await.unwrap()
    }

    async fn dump_tree(self) {
        let db = self.load_db().await;
        // For now, hardcode tree_id to 0
        let tree_id = TreeID(0);
        db.dump_fs(&mut io::stdout(), tree_id).await.unwrap()
    }

    async fn load_db(&self) -> Arc<Database> {
        let dev_manager = DevManager::default();
        for disk in self.disks.iter() {
            dev_manager.taste(disk).await.unwrap();
        }
        let db = dev_manager
            .import_by_name(&self.pool_name)
            .await
            .unwrap_or_else(|_e| {
                eprintln!("Error: pool not found");
                exit(1);
            });
        Arc::new(db)
    }

    async fn main(self) -> Result<()> {
        if self.alloct {
            self.dump_alloct().await;
        } else if self.forest {
            self.dump_forest().await;
        } else if self.fsm {
            self.dump_fsm().await;
        } else if self.ridt {
            self.dump_ridt().await;
        } else if self.tree {
            self.dump_tree().await
        }
        Ok(())
    }
}

#[derive(Parser, Clone, Debug)]
/// Debugging tools
enum DebugCmd {
    DropCache(DropCache),
    Dump(Dump),
}

mod fs {
    use super::*;

    /// Create a new file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Create {
        /// File system name
        pub(super) name:       String,
        /// File system properties, comma delimited
        #[clap(
            short = 'o',
            long,
            //require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) properties: Vec<String>,
    }

    impl Create {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            let props = self
                .properties
                .iter()
                .map(|ps| {
                    Property::from_str(ps.as_str()).unwrap_or_else(|_e| {
                        eprintln!("Invalid property specification {ps}");
                        std::process::exit(2);
                    })
                })
                .collect::<Vec<_>>();
            bfffs.fs_create(self.name, props).await.map(drop)
        }
    }

    /// Destroy a file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Destroy {
        /// File system name
        pub(super) name: String,
    }

    impl Destroy {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            bfffs.fs_destroy(self.name).await
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum GetField {
        Name,
        Property,
        Value,
        Source,
    }

    impl fmt::Display for GetField {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Name => "NAME".fmt(f),
                Self::Property => "PROPERTY".fmt(f),
                Self::Value => "VALUE".fmt(f),
                Self::Source => "SOURCE".fmt(f),
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub struct FromStrError {}
    impl fmt::Display for FromStrError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Invalid column name")
        }
    }
    impl std::error::Error for FromStrError {}

    impl FromStr for GetField {
        type Err = FromStrError;

        fn from_str(s: &str) -> std::result::Result<Self, FromStrError> {
            match s {
                "name" => Ok(GetField::Name),
                "property" => Ok(GetField::Property),
                "value" => Ok(GetField::Value),
                "source" => Ok(GetField::Source),
                _ => Err(FromStrError {}),
            }
        }
    }

    /// Get dataset properties
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Get {
        #[clap(short = 'p', long, help = "Scriptable output")]
        pub(super) parseable:  bool,
        #[clap(
            short = 'o',
            long,
            help = "Fields to display",
            default_value = "name,property,value,source",
            //require_value_delimiter(true),
            value_delimiter(','),
            //multiple_occurrences = false
        )]
        pub(super) fields:     Vec<GetField>,
        #[clap(
            short = 's',
            long,
            help = "Only display properties coming from these sources.",
            default_value = "local,default,inherited,none",
            //require_value_delimiter(true),
            value_delimiter(','),
            //multiple_occurrences = false
        )]
        pub(super) sources:    Vec<PropertySource>,
        /// Recursively display all children
        #[clap(short = 'r', long)]
        pub(super) recursive:  bool,
        /// Recursively display children up to this many levels deep
        #[clap(short = 'd', long)]
        pub(super) depth:      Option<usize>,
        /// Dataset properties to display, comma delimited
        #[clap(
            action = ArgAction::Set,
            num_args(1),
            required(true),
            value_delimiter(','),
        )]
        pub(super) properties: Vec<PropertyName>,
        /// Datasets to inspect, comma delimited
        #[clap( num_args(1..), required(true))]
        pub(super) datasets:   Vec<String>,
    }

    impl Get {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            let depth = self.depth.unwrap_or(if self.recursive {
                usize::MAX
            } else {
                0
            });
            let mut all = Vec::new();
            for ds in self.datasets.into_iter() {
                bfffs
                    .fs_list(ds, self.properties.clone(), None, depth)
                    .try_for_each(|mut dsinfo| {
                        dsinfo.props.retain(|(_prop, source)| {
                            // We use FROM_PARENT as a shorthand for "any
                            // level of inheritance".
                            let eff_source = if source.is_inherited() {
                                &PropertySource::FROM_PARENT
                            } else {
                                source
                            };
                            self.sources.contains(eff_source)
                        });
                        all.push(dsinfo);
                        future::ok(())
                    })
                    .await?;
            }
            // Sort datasets by name, until other sort options are added
            all.sort_unstable_by(|x, y| x.name.cmp(&y.name));

            if self.parseable {
                let stdout = io::stdout();
                let lock = stdout.lock();
                let mut buf = io::BufWriter::new(lock);
                for dsinfo in all {
                    for (prop, source) in dsinfo.props {
                        let mut row = Vec::new();
                        for field in &self.fields {
                            match field {
                                GetField::Name => row.push(dsinfo.name.clone()),
                                GetField::Property => {
                                    row.push(prop.name().to_string())
                                }
                                GetField::Value => row.push(prop.to_string()),
                                GetField::Source => {
                                    row.push(source.to_string())
                                }
                            };
                        }
                        writeln!(buf, "{}", row.join("\t")).unwrap();
                    }
                }
                buf.flush().unwrap();
            } else {
                let row_spec = vec!["{:<}"; self.fields.len()].join(" ");
                let mut table = tabular::Table::new(&row_spec);
                let mut hrow = tabular::Row::new();
                for field in &self.fields {
                    hrow.add_cell(field);
                }
                table.add_row(hrow);

                for dsinfo in all {
                    for (prop, source) in dsinfo.props {
                        let mut row = tabular::Row::new();
                        for field in &self.fields {
                            match field {
                                GetField::Name => row.add_cell(&dsinfo.name),
                                GetField::Property => row.add_cell(prop.name()),
                                GetField::Value => {
                                    let hprop = humanize_property(&prop);
                                    row.add_cell(hprop)
                                }
                                GetField::Source => row.add_cell(source),
                            };
                        }
                        table.add_row(row);
                    }
                }
                print!("{table}");
            }
            Ok(())
        }
    }

    /// List file systems
    #[derive(Parser, Clone, Debug)]
    pub(super) struct List {
        #[clap(short = 'p', long, help = "Scriptable output")]
        pub(super) parseable:  bool,
        /// Dataset properties to display, comma delimited
        #[clap(
            short = 'o',
            long,
            //require_value_delimiter(true),
            value_delimiter(','),
            default_value = "name"
        )]
        pub(super) properties: Vec<PropertyName>,
        /// Recursively display all children
        #[clap(short = 'r', long)]
        pub(super) recursive:  bool,
        /// Recursively display children up to this many levels deep
        #[clap(short = 'd', long)]
        pub(super) depth:      Option<usize>,
        /// Ascending sort columns, comma delimited
        #[clap(
            short = 's',
            long,
            action = clap::ArgAction::Append,
            default_value = "name"
        )]
        // TODO: add a -S option for descending columns.  But first #[clap]
        // needs to get support for storing argument indices.
        // https://github.com/clap-rs/clap/discussions/3399
        // https://github.com/clap-rs/clap/issues/3846
        pub(super) sort: Vec<PropertyName>,
        pub(super) datasets:   Vec<String>,
    }

    impl List {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let depth = self.depth.unwrap_or(if self.recursive {
                usize::MAX
            } else {
                0
            });

            // This could be written in a functional way once Iterator::try_collect stabilizes.
            // https://github.com/rust-lang/rust/issues/94047
            let mut sort_indices = Vec::<usize>::with_capacity(self.sort.len());
            for sname in self.sort.into_iter() {
                if let Some(i) =
                    self.properties.iter().position(|&pname| sname == pname)
                {
                    sort_indices.push(i);
                } else {
                    eprintln!("Cannot sort by a property that isn't listed");
                    return Err(Error::EINVAL);
                }
            }

            let bfffs = Bfffs::new(sock).await.unwrap();
            let mut all = Vec::new();
            for ds in self.datasets.into_iter() {
                bfffs
                    .fs_list(ds, self.properties.clone(), None, depth)
                    .try_for_each(|dsinfo| {
                        all.push(dsinfo);
                        future::ok(())
                    })
                    .await?;
            }

            all.sort_unstable_by(|x, y| {
                for pidx in &sort_indices {
                    let r = x.props[*pidx].0.cmp(&y.props[*pidx].0);
                    if Ordering::Equal != r {
                        return r;
                    }
                }
                Ordering::Equal
            });

            if self.parseable {
                let stdout = io::stdout();
                let lock = stdout.lock();
                let mut buf = io::BufWriter::new(lock);
                for dsinfo in all {
                    let mut row = Vec::new();
                    for (prop, _source) in dsinfo.props {
                        row.push(format!("{prop}"));
                    }
                    writeln!(buf, "{}", row.join("\t")).unwrap();
                }
                buf.flush().unwrap();
            } else {
                let row_spec = vec!["{:<}"; self.properties.len()];
                let mut table = tabular::Table::new(&row_spec.join(" "));
                let mut hrow = tabular::Row::new();
                for i in 0..(self.properties.len()) {
                    hrow.add_cell(hname(self.properties[i]));
                }
                table.add_row(hrow);

                for dsinfo in all {
                    let mut row = tabular::Row::new();
                    for (prop, _source) in dsinfo.props {
                        let hprop = humanize_property(&prop);
                        row.add_cell(hprop);
                    }
                    table.add_row(row);
                }
                print!("{table}");
            }
            Ok(())
        }
    }

    /// Mount a file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Mount {
        /// Mount options, comma delimited
        #[clap(
            short = 'o',
            long,
            //require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) options: Vec<String>,
        /// File system name, including the pool.
        pub(super) name:    String,
    }

    impl Mount {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            bfffs.fs_mount(self.name).await
        }
    }

    /// Set dataset properties
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Set {
        /// Dataset properties to set, comma delimited
        #[clap(
            action = ArgAction::Set,
            num_args(1),
            required(true),
            value_delimiter(','),
        )]
        pub(super) properties: Vec<Property>,
        /// Datasets to modify, comma delimited
        #[clap(num_args(1..), required(true))]
        pub(super) datasets:   Vec<String>,
    }

    impl Set {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            for ds in self.datasets.into_iter() {
                let bfffs = Bfffs::new(sock).await.unwrap();
                bfffs.fs_set(ds, self.properties.clone()).await?
            }
            Ok(())
        }
    }

    /// Unmount a file system
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Unmount {
        /// Focibly unmount the file system even if files are still active.
        #[clap(short, long)]
        pub(super) force: bool,
        /// File system name, including the pool.
        pub(super) name:  String,
    }

    impl Unmount {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            bfffs.fs_unmount(&self.name, self.force).await
        }
    }

    #[derive(Parser, Clone, Debug)]
    /// Create, destroy, and modify file systems
    pub(super) enum FsCmd {
        Create(Create),
        Destroy(Destroy),
        Get(Get),
        List(List),
        Mount(Mount),
        Set(Set),
        Unmount(Unmount),
    }

    fn hname(propname: PropertyName) -> &'static str {
        match propname {
            PropertyName::Atime => "ATIME",
            PropertyName::BaseMountpoint => "BASEMOUNTPOINT",
            PropertyName::Mountpoint => "MOUNTPOINT",
            PropertyName::Name => "NAME",
            PropertyName::RecordSize => "RECSIZE",
        }
    }

    si_scale::scale_fn!(bibytes0,
                                 base: B1024,
                                 constraint: UnitAndAbove,
                                 mantissa_fmt: "{:.0}",
                                 groupings: '_',
                                 unit: "B");

    fn humanize_property(prop: &Property) -> String {
        match prop {
            Property::Atime(b) => {
                match b {
                    true => String::from("on"),
                    false => String::from("off"),
                }
            }
            Property::BaseMountpoint(s) => s.to_owned(),
            Property::Mountpoint(s) => s.to_owned(),
            Property::Name(s) => s.to_owned(),
            Property::RecordSize(i) => bibytes0(1 << i),
        }
    }
}

mod pool {
    use std::{num::NonZeroU64, sync::Mutex};

    use bfffs_core::{
        cache::Cache,
        cluster::Cluster,
        database::*,
        ddml::DDML,
        idml::IDML,
        mirror::Mirror,
        pool::Pool,
        raid,
        BYTES_PER_LBA,
    };

    use super::*;

    /// Clean freed space on a pool
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Clean {
        /// Pool name
        pub(super) pool_name: String,
    }

    impl Clean {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            bfffs.pool_clean(self.pool_name).await
        }
    }

    /// Create a new storage pool
    #[derive(Parser, Clone, Debug)]
    pub(super) struct Create {
        /// Dataset properties, comma delimited
        #[clap(
            short,
            long,
            //require_value_delimiter(true),
            value_delimiter(',')
        )]
        pub(super) properties: Vec<String>,
        /// Simulated zone size in MB
        #[clap(long)]
        pub(super) zone_size:  Option<u64>,
        #[clap(required(true))]
        /// Pool name
        pub(super) pool_name:  String,
        #[clap(required(true))]
        pub(super) vdev:       Vec<String>,
    }

    use lalrpop_util::lalrpop_mod;
    lalrpop_mod!(#[allow(clippy::just_underscores_and_digits)] pub pool_create_parser);
    use pool_create_parser::PoolParser;

    use crate::pool_create_ast::{RaidChild, Tlv};

    impl Create {
        pub(super) async fn main(self) -> Result<()> {
            let zone_size = self.zone_size.map(|mbs| {
                let lbas = mbs * 1024 * 1024 / (BYTES_PER_LBA as u64);
                NonZeroU64::new(lbas).expect("zone_size may not be zero")
            });

            let props = self.properties.iter().map(String::as_str);
            let mut builder = Builder::new(self.pool_name, props, zone_size);
            let all_vdevs = self.vdev.join(" ");
            let spec = PoolParser::new().parse(&all_vdevs).unwrap();
            for tvd in spec.0 {
                match tvd {
                    Tlv::Raid(r) => {
                        for child in r.vdevs {
                            match child {
                                RaidChild::Mirror(mchild) => {
                                    builder.create_mirror(&mchild.0[..]);
                                }
                                RaidChild::Disk(dchild) => {
                                    builder.create_mirror(&[dchild.0]);
                                }
                            }
                        }
                        builder.create_cluster(r.k, r.f);
                    }
                    Tlv::Mirror(m) => builder.create_mirror_tlv(&m.0[..]),
                    Tlv::Disk(d) => builder.create_single(d),
                }
            }
            builder.format().await;
            Ok(())
        }
    }

    struct Builder {
        clusters:   Vec<Cluster>,
        mirrors:    Vec<Mirror>,
        name:       String,
        properties: Vec<Property>,
        zone_size:  Option<NonZeroU64>,
    }

    impl Builder {
        pub fn new<'a, P>(
            name: String,
            propstrings: P,
            zone_size: Option<NonZeroU64>,
        ) -> Self
        where
            P: Iterator<Item = &'a str> + 'a,
        {
            let clusters = Vec::new();
            let mirrors = Vec::new();
            let properties = propstrings
                .map(|ps| {
                    Property::from_str(ps).unwrap_or_else(|_e| {
                        eprintln!("Invalid property specification {ps}");
                        std::process::exit(2);
                    })
                })
                .collect::<Vec<_>>();
            Builder {
                clusters,
                mirrors,
                name,
                properties,
                zone_size,
            }
        }

        pub fn create_mirror(&mut self, devs: &[&str]) {
            self.mirrors
                .push(Mirror::create(devs, self.zone_size).unwrap());
        }

        pub fn create_mirror_tlv(&mut self, devs: &[&str]) {
            self.create_mirror(devs);
            self.create_cluster(1, 0);
        }

        pub fn create_single(&mut self, dev: &str) {
            self.create_mirror(&[dev]);
            self.create_cluster(1, 0)
        }

        pub fn create_cluster(&mut self, k: i16, f: i16) {
            let mirrors = mem::take(&mut self.mirrors);
            let raid = raid::create(None, k, f, mirrors);
            let c = Cluster::create(raid);
            self.clusters.push(c);
        }

        /// Actually format the disks
        pub async fn format(mut self) {
            let name = self.name.clone();
            let clusters = self.clusters.drain(..).collect();
            let pool = Pool::create(name, clusters);
            let cache = Arc::new(Mutex::new(Cache::with_capacity(4_194_304)));
            let ddml = Arc::new(DDML::new(pool, cache.clone()));
            let idml = Arc::new(IDML::create(ddml, cache));
            let db = Database::create(idml);
            let controller = Controller::new(db);
            // Create the root file system
            controller.create_fs(&self.name).await.unwrap();
            for prop in self.properties.into_iter() {
                controller.set_prop(&self.name, prop).await.unwrap();
            }
            controller.sync_transaction().await.unwrap();
        }
    }

    /// List one or more storage pools
    #[derive(Parser, Clone, Debug)]
    pub(super) struct List {
        /// Pool properties, comma delimited
        #[clap(short, long, value_delimiter(','))]
        pub(super) properties: Vec<String>,
        /// Optional pool names
        pub(super) pools:      Vec<String>,
    }

    impl List {
        pub(super) async fn main(self, sock: &Path) -> Result<()> {
            let bfffs = Bfffs::new(sock).await.unwrap();
            println!("NAME");
            if self.pools.is_empty() {
                bfffs
                    .pool_list(None, None)
                    .try_for_each(|poolinfo| {
                        println!("{}", poolinfo.name);
                        future::ok(())
                    })
                    .await?;
            } else {
                for pool in self.pools.into_iter() {
                    bfffs
                        .pool_list(Some(pool), None)
                        .try_for_each(|poolinfo| {
                            println!("{}", poolinfo.name);
                            future::ok(())
                        })
                        .await?;
                }
            }
            Ok(())
        }
    }

    #[derive(Parser, Clone, Debug)]
    /// Create, destroy, and modify storage pools
    pub(super) enum PoolCmd {
        Clean(Clean),
        Create(Create),
        List(List),
    }
}

#[derive(Parser, Clone, Debug)]
enum SubCommand {
    Check(Check),
    #[clap(subcommand)]
    Debug(DebugCmd),
    #[clap(subcommand)]
    Fs(fs::FsCmd),
    #[clap(subcommand)]
    Pool(pool::PoolCmd),
}

#[derive(Parser, Clone, Debug)]
#[clap(version = crate_version!())]
struct Cli {
    /// Path to the bfffsd socket
    #[clap(long, default_value = "/var/run/bfffsd.sock")]
    sock: PathBuf,
    #[clap(subcommand)]
    cmd:  SubCommand,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let cli: Cli = Cli::parse();
    match cli.cmd {
        SubCommand::Check(check) => check.main().await,
        SubCommand::Fs(fs::FsCmd::Create(create)) => {
            create.main(&cli.sock).await
        }
        SubCommand::Fs(fs::FsCmd::Destroy(destroy)) => {
            destroy.main(&cli.sock).await
        }
        SubCommand::Fs(fs::FsCmd::Get(get)) => get.main(&cli.sock).await,
        SubCommand::Fs(fs::FsCmd::List(list)) => list.main(&cli.sock).await,
        SubCommand::Fs(fs::FsCmd::Mount(mount)) => mount.main(&cli.sock).await,
        SubCommand::Fs(fs::FsCmd::Set(set)) => set.main(&cli.sock).await,
        SubCommand::Fs(fs::FsCmd::Unmount(unmount)) => {
            unmount.main(&cli.sock).await
        }
        SubCommand::Debug(DebugCmd::DropCache(dc)) => dc.main(&cli.sock).await,
        SubCommand::Debug(DebugCmd::Dump(dump)) => dump.main().await,
        SubCommand::Pool(pool::PoolCmd::Create(create)) => create.main().await,
        SubCommand::Pool(pool::PoolCmd::Clean(clean)) => {
            clean.main(&cli.sock).await
        }
        SubCommand::Pool(pool::PoolCmd::List(list)) => {
            list.main(&cli.sock).await
        }
    }
}

#[cfg(test)]
mod t {
    use clap::error::ErrorKind::*;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(Vec::new())]
    #[case(vec!["bfffs"])]
    #[case(vec!["bfffs", "check"])]
    #[case(vec!["bfffs", "check", "testpool"])]
    #[case(vec!["bfffs", "debug"])]
    #[case(vec!["bfffs", "debug", "dump"])]
    #[case(vec!["bfffs", "debug", "dump", "testpool"])]
    #[case(vec!["bfffs", "fs", "create"])]
    #[case(vec!["bfffs", "pool"])]
    #[case(vec!["bfffs", "pool", "create"])]
    #[case(vec!["bfffs", "pool", "create", "testpool"])]
    fn missing_arg(#[case] args: Vec<&str>) {
        let e = Cli::try_parse_from(args).unwrap_err();
        assert!(
            e.kind() == MissingRequiredArgument ||
                e.kind() == DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn check() {
        let args = vec!["bfffs", "check", "testpool", "/dev/da0", "/dev/da1"];
        let cli = Cli::try_parse_from(args).unwrap();
        assert!(matches!(cli.cmd, SubCommand::Check(_)));
        if let SubCommand::Check(check) = cli.cmd {
            assert_eq!(check.pool_name, "testpool");
            assert_eq!(check.disks[0], Path::new("/dev/da0"));
            assert_eq!(check.disks[1], Path::new("/dev/da1"));
        }
    }

    mod debug {
        use super::*;

        #[test]
        fn drop_cache() {
            let args = vec!["bfffs", "debug", "drop-cache"];
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.cmd, SubCommand::Debug(_)));
        }

        #[test]
        fn dump_fsm() {
            let args = vec![
                "bfffs", "debug", "dump", "-f", "testpool", "/dev/da0",
                "/dev/da1",
            ];
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.cmd, SubCommand::Debug(_)));
            if let SubCommand::Debug(DebugCmd::Dump(debug)) = cli.cmd {
                assert_eq!(debug.pool_name, "testpool");
                assert!(debug.fsm);
                assert!(!debug.tree);
                assert_eq!(debug.disks[0], Path::new("/dev/da0"));
                assert_eq!(debug.disks[1], Path::new("/dev/da1"));
            }
        }

        #[test]
        fn dump_tree() {
            let args = vec![
                "bfffs", "debug", "dump", "-t", "testpool", "/dev/da0",
                "/dev/da1",
            ];
            let cli = Cli::try_parse_from(args).unwrap();
            assert!(matches!(cli.cmd, SubCommand::Debug(_)));
            if let SubCommand::Debug(DebugCmd::Dump(debug)) = cli.cmd {
                assert_eq!(debug.pool_name, "testpool");
                assert!(!debug.fsm);
                assert!(debug.tree);
                assert_eq!(debug.disks[0], Path::new("/dev/da0"));
                assert_eq!(debug.disks[1], Path::new("/dev/da1"));
            }
        }
    }

    mod fs {
        use super::*;
        use crate::fs::*;

        mod create {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "create", "testpool/foo"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Create(_))));
                if let SubCommand::Fs(FsCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.name, "testpool/foo");
                    assert!(create.properties.is_empty());
                }
            }

            #[test]
            fn props() {
                let args = vec![
                    "bfffs",
                    "fs",
                    "create",
                    "-o",
                    "atime=off,recsize=65536",
                    "testpool/foo",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Create(_))));
                if let SubCommand::Fs(FsCmd::Create(create)) = cli.cmd {
                    assert_eq!(
                        create.properties,
                        vec!["atime=off", "recsize=65536"]
                    );
                }
            }
        }

        mod destroy {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "destroy", "testpool/foo"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Destroy(_))));
                if let SubCommand::Fs(FsCmd::Destroy(destroy)) = cli.cmd {
                    assert_eq!(destroy.name, "testpool/foo");
                }
            }
        }

        mod get {
            use super::*;
            use crate::fs;

            #[test]
            fn depth() {
                let args =
                    vec!["bfffs", "fs", "get", "-d", "42", "atime", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(get.depth, Some(42));
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(&get.properties[..], &[PropertyName::Atime][..]);
                }
            }

            #[test]
            fn fields() {
                let args = vec![
                    "bfffs",
                    "fs",
                    "get",
                    "-o",
                    "name,property",
                    "atime",
                    "testpool",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(
                        &get.fields[..],
                        &[fs::GetField::Name, fs::GetField::Property]
                    );
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(&get.properties[..], &[PropertyName::Atime][..]);
                }
            }

            #[test]
            fn parseable() {
                let args =
                    vec!["bfffs", "fs", "get", "-p", "atime", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert!(get.parseable);
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(&get.properties[..], &[PropertyName::Atime][..]);
                }
            }

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "get", "recordsize", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(
                        &get.properties[..],
                        &[PropertyName::RecordSize][..]
                    );
                }
            }

            #[test]
            fn recursive() {
                let args =
                    vec!["bfffs", "fs", "get", "-r", "atime", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert!(get.recursive);
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(&get.properties[..], &[PropertyName::Atime][..]);
                }
            }

            #[test]
            fn sources() {
                let args = vec![
                    "bfffs",
                    "fs",
                    "get",
                    "-s",
                    "default",
                    "recordsize",
                    "testpool",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(&get.sources[..], &[PropertySource::Default]);
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(
                        &get.properties[..],
                        &[PropertyName::RecordSize][..]
                    );
                }
            }

            #[test]
            fn twodatasets() {
                let args = vec!["bfffs", "fs", "get", "atime", "foo", "bar"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(&get.datasets[..], &["foo", "bar"][..]);
                    assert_eq!(&get.properties[..], &[PropertyName::Atime][..]);
                }
            }

            #[test]
            fn twoprops() {
                let args =
                    vec!["bfffs", "fs", "get", "recsize,atime", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Get(_))));
                if let SubCommand::Fs(FsCmd::Get(get)) = cli.cmd {
                    assert_eq!(&get.datasets[..], &["testpool"][..]);
                    assert_eq!(
                        &get.properties[..],
                        &[PropertyName::RecordSize, PropertyName::Atime][..]
                    );
                }
            }
        }

        mod list {
            use super::*;

            #[test]
            fn sort() {
                let args = vec![
                    "bfffs", "fs", "list", "-s", "recsize", "-s", "atime",
                    "-s", "name", "testpool",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::List(_))));
                if let SubCommand::Fs(FsCmd::List(list)) = cli.cmd {
                    assert_eq!(list.datasets, &["testpool"]);
                    assert_eq!(
                        list.sort,
                        &[
                            PropertyName::RecordSize,
                            PropertyName::Atime,
                            PropertyName::Name
                        ]
                    );
                }
            }
        }

        mod mount {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "mount", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Mount(_))));
                if let SubCommand::Fs(FsCmd::Mount(mount)) = cli.cmd {
                    assert_eq!(mount.name, "testpool");
                    assert!(mount.options.is_empty());
                }
            }

            #[test]
            fn subfs() {
                let args = vec!["bfffs", "fs", "mount", "testpool/foo"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Mount(_))));
                if let SubCommand::Fs(FsCmd::Mount(mount)) = cli.cmd {
                    assert_eq!(mount.name, "testpool/foo");
                    assert!(mount.options.is_empty());
                }
            }
        }

        mod set {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "set", "atime=off", "mypool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Set(_))));
                if let SubCommand::Fs(FsCmd::Set(set)) = cli.cmd {
                    assert_eq!(&set.datasets[..], &["mypool"][..]);
                    assert_eq!(
                        &set.properties[..],
                        &[Property::Atime(false)][..]
                    );
                }
            }
        }

        mod unmount {
            use super::*;

            #[test]
            fn force() {
                let args = vec!["bfffs", "fs", "unmount", "-f", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Unmount(_))));
                if let SubCommand::Fs(FsCmd::Unmount(unmount)) = cli.cmd {
                    assert_eq!(unmount.name, "testpool");
                    assert!(unmount.force);
                }
            }

            #[test]
            fn plain() {
                let args = vec!["bfffs", "fs", "unmount", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Unmount(_))));
                if let SubCommand::Fs(FsCmd::Unmount(unmount)) = cli.cmd {
                    assert_eq!(unmount.name, "testpool");
                    assert!(!unmount.force);
                }
            }

            #[test]
            fn subfs() {
                let args = vec!["bfffs", "fs", "unmount", "testpool/foo"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Fs(FsCmd::Unmount(_))));
                if let SubCommand::Fs(FsCmd::Unmount(unmount)) = cli.cmd {
                    assert_eq!(unmount.name, "testpool/foo");
                    assert!(!unmount.force);
                }
            }
        }
    }

    mod pool {
        use super::*;
        use crate::pool::*;

        mod create {
            use super::*;

            /// test parsing the vdev specification of "bfffs pool create"
            mod lalrpop {
                use crate::{pool::pool_create_parser::*, pool_create_ast::*};

                #[test]
                fn disk() {
                    let pool = PoolParser::new().parse("/dev/da0").unwrap();
                    assert_eq!(pool, Pool(vec![Tlv::Disk("/dev/da0")]));
                }

                #[test]
                fn mirror() {
                    let pool = PoolParser::new()
                        .parse("mirror /dev/da0 /tmp/bfffs.img")
                        .unwrap();
                    assert_eq!(
                        pool,
                        Pool(vec![Tlv::Mirror(Mirror(vec![
                            "/dev/da0",
                            "/tmp/bfffs.img"
                        ]))])
                    );
                }

                #[test]
                fn raid5() {
                    let pool = PoolParser::new()
                        .parse("raid 3 1 /dev/da0 /tmp/bfffs.img /dev/da1")
                        .unwrap();
                    assert_eq!(
                        pool,
                        Pool(vec![Tlv::Raid(Raid {
                            k:     3,
                            f:     1,
                            vdevs: vec![
                                RaidChild::Disk(Disk("/dev/da0")),
                                RaidChild::Disk(Disk("/tmp/bfffs.img")),
                                RaidChild::Disk(Disk("/dev/da1"))
                            ],
                        })])
                    );
                }

                #[test]
                fn raid51() {
                    let pool = PoolParser::new()
                        .parse("raid 3 1 mirror da0 da1 mirror da2 da3 mirror da4 da5")
                        .unwrap();
                    assert_eq!(
                        pool,
                        Pool(vec![Tlv::Raid(Raid {
                            k:     3,
                            f:     1,
                            vdevs: vec![
                                RaidChild::Mirror(Mirror(vec!["da0", "da1"])),
                                RaidChild::Mirror(Mirror(vec!["da2", "da3"])),
                                RaidChild::Mirror(Mirror(vec!["da4", "da5"])),
                            ],
                        })])
                    );
                }

                #[test]
                fn raid50() {
                    let pool = PoolParser::new()
                        .parse("raid 3 1 /dev/da0 /dev/da1 /dev/da2 raid 3 1 /dev/da3 /dev/da4 /dev/da5")
                        .unwrap();
                    assert_eq!(
                        pool,
                        Pool(vec![
                            Tlv::Raid(Raid {
                                k:     3,
                                f:     1,
                                vdevs: vec![
                                    RaidChild::Disk(Disk("/dev/da0")),
                                    RaidChild::Disk(Disk("/dev/da1")),
                                    RaidChild::Disk(Disk("/dev/da2")),
                                ],
                            },),
                            Tlv::Raid(Raid {
                                k:     3,
                                f:     1,
                                vdevs: vec![
                                    RaidChild::Disk(Disk("/dev/da3")),
                                    RaidChild::Disk(Disk("/dev/da4")),
                                    RaidChild::Disk(Disk("/dev/da5")),
                                ],
                            },),
                        ],)
                    );
                }
            }

            #[test]
            fn plain() {
                let args =
                    vec!["bfffs", "pool", "create", "testpool", "/dev/da0"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.pool_name, "testpool");
                    assert!(create.properties.is_empty());
                    assert!(create.zone_size.is_none());
                    assert_eq!(create.vdev[0], "/dev/da0");
                }
            }

            #[test]
            fn props() {
                let args = vec![
                    "bfffs",
                    "pool",
                    "create",
                    "-p",
                    "atime=off,recsize=65536",
                    "testpool",
                    "/dev/da0",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(
                        create.properties,
                        vec!["atime=off", "recsize=65536"]
                    );
                }
            }

            #[test]
            fn zone_size() {
                let args = vec![
                    "bfffs",
                    "pool",
                    "create",
                    "--zone-size",
                    "128",
                    "testpool",
                    "/dev/da0",
                ];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(
                    cli.cmd,
                    SubCommand::Pool(PoolCmd::Create(_))
                ));
                if let SubCommand::Pool(PoolCmd::Create(create)) = cli.cmd {
                    assert_eq!(create.zone_size, Some(128));
                }
            }
        }

        mod list {
            use super::*;

            #[test]
            fn plain() {
                let args = vec!["bfffs", "pool", "list", "testpool"];
                let cli = Cli::try_parse_from(args).unwrap();
                assert!(matches!(cli.cmd, SubCommand::Pool(PoolCmd::List(_))));
                if let SubCommand::Pool(PoolCmd::List(list)) = cli.cmd {
                    assert_eq!(list.pools.len(), 1);
                    assert_eq!(list.pools[0], "testpool");
                    assert!(list.properties.is_empty());
                }
            }
        }
    }
}
