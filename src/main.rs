// SPDX-License-Identifier: Apache-2.0
// Copyright (c) NVIDIA CORPORATION

mod ffi;
mod fm;
mod types;

use std::collections::BTreeSet;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use fm::{FmAddress, FmConnection, FmLib};

#[derive(Clone, ValueEnum)]
enum OutputFormat {
    Table,
    Json,
}

const VERSION: &str = "2.0.0";
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_TIMEOUT_MS: u32 = 1000;

#[derive(Parser)]
#[command(version = VERSION, about = "NVIDIA Fabric Manager client")]
struct Cli {
    /// List supported fabric partitions
    #[arg(short = 'l')]
    list: bool,

    /// Activate a fabric partition
    #[arg(short = 'a', value_name = "ID")]
    activate: Option<u32>,

    /// Deactivate a fabric partition
    #[arg(short = 'd', value_name = "ID")]
    deactivate: Option<u32>,

    /// Hostname or IP address (TCP) of Fabric Manager.
    ///
    /// Requires FM_CMD_BIND_INTERFACE=<IP> in Fabric Manager configuration.
    #[arg(short = 'i', long, value_name = "HOST", default_value = DEFAULT_HOST)]
    hostname: Option<String>,

    /// Unix domain socket path for Fabric Manager connection.
    ///
    /// Overrides --hostname when specified.
    /// Requires FM_CMD_UNIX_SOCKET_PATH=<path> in Fabric Manager configuration.
    #[arg(short = 's', long = "unix-domain-socket", value_name = "PATH")]
    unix_socket: Option<String>,

    /// vsock address (CID:PORT) for host-to-guest VM communication.
    ///
    /// Overrides --hostname and --unix-domain-socket when specified.
    /// Requires FM_CMD_VSOCK_CID and FM_CMD_VSOCK_PORT_NUMBER in guest FM configuration.
    #[arg(short = 'v', long, value_name = "CID:PORT")]
    vsock: Option<String>,

    /// Query all NVLink failed devices
    #[arg(short = 'g', long = "get-nvlink-failed-devices")]
    nvlink_failed: bool,

    /// List unsupported fabric partitions
    #[arg(short = 'u', long = "list-unsupported-partitions")]
    list_unsupported: bool,

    /// Activate a partition with VFs (BDF format: PARTITION_ID=DDDD:BB:DD.F,...).
    ///
    /// Host PCI addresses of SR-IOV Virtual Functions passed through to guest VMs.
    /// The order of VFs must correspond to the physical GPUs in the partition.
    /// Requires FABRIC_MODE=2 in Fabric Manager configuration.
    #[arg(long = "activate-with-vfs", value_name = "ID=BDFs")]
    activate_with_vfs: Option<String>,

    /// Set activated partition list (comma-separated, no spaces).
    ///
    /// Used for Fabric Manager resiliency restart mode.
    /// Requires FABRIC_MODE_RESTART=1 in Fabric Manager configuration.
    #[arg(short = 'p', long = "set-activated-list", value_name = "IDS")]
    set_activated: Option<String>,

    /// Output format [possible: table, json]
    #[arg(
        short = 'o',
        long,
        value_name = "FMT",
        default_value = "table",
        hide_possible_values = true
    )]
    output: OutputFormat,

    /// Timeout in seconds to wait for Fabric Manager to become available
    #[arg(short = 't', long, value_name = "SECS", default_value = "30")]
    timeout: u64,
}

fn parse_partition_ids(input: &str) -> Result<Vec<u32>, String> {
    input
        .split(',')
        .map(|s| {
            s.trim()
                .parse::<u32>()
                .map_err(|_| format!("invalid partition id: {s}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .map(|set| set.into_iter().collect())
}

fn parse_bdf(bdf: &str) -> Result<ffi::FmPciDevice, String> {
    // DDDD:BB:DD.F
    let parts: Vec<&str> = bdf.split(':').collect();
    if parts.len() != 3 {
        return Err(format!("invalid BDF format: {bdf} (expected DDDD:BB:DD.F)"));
    }
    let domain =
        u32::from_str_radix(parts[0], 16).map_err(|_| format!("invalid domain in BDF: {bdf}"))?;
    let bus =
        u32::from_str_radix(parts[1], 16).map_err(|_| format!("invalid bus in BDF: {bdf}"))?;
    let dev_fn: Vec<&str> = parts[2].split('.').collect();
    if dev_fn.len() != 2 {
        return Err(format!("invalid device.function in BDF: {bdf}"));
    }
    let device =
        u32::from_str_radix(dev_fn[0], 16).map_err(|_| format!("invalid device in BDF: {bdf}"))?;
    let function = u32::from_str_radix(dev_fn[1], 16)
        .map_err(|_| format!("invalid function in BDF: {bdf}"))?;
    Ok(ffi::FmPciDevice {
        domain,
        bus,
        device,
        function,
    })
}

fn parse_activate_with_vfs(input: &str) -> Result<(u32, Vec<ffi::FmPciDevice>), String> {
    let (id_str, bdfs_str) = input
        .split_once('=')
        .ok_or_else(|| format!("expected PARTITION_ID=BDF,BDF,...  got: {input}"))?;
    let partition_id = id_str
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("invalid partition id: {id_str}"))?;
    let vfs = bdfs_str
        .split(',')
        .map(|s| parse_bdf(s.trim()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((partition_id, vfs))
}

fn output<T: serde::Serialize>(value: &T, table: &dyn std::fmt::Display, format: &OutputFormat) {
    match format {
        OutputFormat::Json => println!(
            "{}",
            serde_json::to_string_pretty(value).expect("serialization cannot fail")
        ),
        OutputFormat::Table => print!("{table}"),
    }
}

fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let lib = FmLib::init()?;

    let address = if let Some(ref addr) = cli.vsock {
        FmAddress::Vsock(addr)
    } else if let Some(ref path) = cli.unix_socket {
        FmAddress::Unix(path)
    } else {
        FmAddress::Tcp(cli.hostname.as_deref().unwrap_or(DEFAULT_HOST))
    };

    let conn = FmConnection::connect(
        &lib,
        address,
        DEFAULT_TIMEOUT_MS,
        Duration::from_secs(cli.timeout),
    )?;

    if cli.list {
        let list = conn.list_partitions()?;
        output(&list, &types::Table(&list), &cli.output);
    } else if let Some(id) = cli.activate {
        conn.activate_partition(id)?;
    } else if let Some(id) = cli.deactivate {
        conn.deactivate_partition(id)?;
    } else if let Some(ref vf_str) = cli.activate_with_vfs {
        let (id, vfs) = parse_activate_with_vfs(vf_str)?;
        conn.activate_partition_with_vfs(id, &vfs)?;
    } else if cli.list_unsupported {
        let list = conn.list_unsupported_partitions()?;
        output(&list, &types::Table(&list), &cli.output);
    } else if cli.nvlink_failed {
        let devices = conn.get_nvlink_failed_devices()?;
        output(&devices, &devices, &cli.output);
    } else if let Some(ref ids_str) = cli.set_activated {
        let ids = parse_partition_ids(ids_str)?;
        conn.set_activated_partitions(&ids)?;
    }

    Ok(())
}

fn has_operation(cli: &Cli) -> bool {
    cli.list
        || cli.activate.is_some()
        || cli.deactivate.is_some()
        || cli.activate_with_vfs.is_some()
        || cli.nvlink_failed
        || cli.list_unsupported
        || cli.set_activated.is_some()
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    if !has_operation(&cli) {
        Cli::parse_from(["fmclient", "--help"]);
    }

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
