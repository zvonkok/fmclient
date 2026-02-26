// SPDX-License-Identifier: Apache-2.0
// Copyright (c) NVIDIA CORPORATION

use std::ffi::CStr;
use std::fmt;
use std::os::raw::c_char;

use serde::Serialize;

use crate::ffi;

fn cstr_from_buf(buf: &[c_char]) -> String {
    unsafe { CStr::from_ptr(buf.as_ptr()) }
        .to_string_lossy()
        .into_owned()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionList {
    pub version: u32,
    pub num_partitions: u32,
    pub max_num_partitions: u32,
    pub partition_info: Vec<PartitionInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    pub partition_id: u32,
    pub is_active: u32,
    pub num_gpus: u32,
    pub gpu_info: Vec<GpuInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GpuInfo {
    pub physical_id: u32,
    pub uuid: String,
    pub pci_bus_id: String,
    pub num_nv_links_available: u32,
    pub max_num_nv_links: u32,
    pub nvlink_line_rate_m_bps: u32,
}

impl From<&ffi::FmFabricPartitionGpuInfo> for GpuInfo {
    fn from(g: &ffi::FmFabricPartitionGpuInfo) -> Self {
        Self {
            physical_id: g.physical_id,
            uuid: cstr_from_buf(&g.uuid),
            pci_bus_id: cstr_from_buf(&g.pci_bus_id),
            num_nv_links_available: g.num_nvlinks_available,
            max_num_nv_links: g.max_num_nvlinks,
            nvlink_line_rate_m_bps: g.nvlink_line_rate_mbps,
        }
    }
}

impl From<&ffi::FmFabricPartitionInfo> for PartitionInfo {
    fn from(p: &ffi::FmFabricPartitionInfo) -> Self {
        Self {
            partition_id: p.partition_id,
            is_active: p.is_active,
            num_gpus: p.num_gpus,
            gpu_info: p.gpu_info[..p.num_gpus as usize]
                .iter()
                .map(GpuInfo::from)
                .collect(),
        }
    }
}

impl From<&ffi::FmFabricPartitionList> for PartitionList {
    fn from(l: &ffi::FmFabricPartitionList) -> Self {
        Self {
            version: l.version,
            num_partitions: l.num_partitions,
            max_num_partitions: l.max_num_partitions,
            partition_info: l.partition_info[..l.num_partitions as usize]
                .iter()
                .map(PartitionInfo::from)
                .collect(),
        }
    }
}

impl fmt::Display for PartitionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Partitions ({} supported, max {}):\n",
            self.num_partitions, self.max_num_partitions
        )?;
        writeln!(
            f,
            "  {:>3}  {:>6}  {:>4}  {:>7}  {:>11}  GPU IDs",
            "ID", "Active", "GPUs", "NVLinks", "Rate (MB/s)"
        )?;
        for p in &self.partition_info {
            let (avail, max, rate) = p.gpu_info.first().map_or((0, 0, 0), |g| {
                (
                    g.num_nv_links_available,
                    g.max_num_nv_links,
                    g.nvlink_line_rate_m_bps,
                )
            });
            let gpu_ids: Vec<String> = p
                .gpu_info
                .iter()
                .map(|g| g.physical_id.to_string())
                .collect();
            writeln!(
                f,
                "  {:>3}  {:>6}  {:>4}  {:>3}/{:<3}  {:>11}  {}",
                p.partition_id,
                if p.is_active != 0 { "yes" } else { "no" },
                p.num_gpus,
                avail,
                max,
                rate,
                gpu_ids.join(",")
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsupportedPartitionList {
    pub version: u32,
    pub num_partitions: u32,
    pub partition_info: Vec<UnsupportedPartitionInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnsupportedPartitionInfo {
    pub partition_id: u32,
    pub num_gpus: u32,
    pub gpu_physical_ids: Vec<u32>,
}

impl From<&ffi::FmUnsupportedFabricPartitionInfo> for UnsupportedPartitionInfo {
    fn from(p: &ffi::FmUnsupportedFabricPartitionInfo) -> Self {
        Self {
            partition_id: p.partition_id,
            num_gpus: p.num_gpus,
            gpu_physical_ids: p.gpu_physical_ids[..p.num_gpus as usize].to_vec(),
        }
    }
}

impl From<&ffi::FmUnsupportedFabricPartitionList> for UnsupportedPartitionList {
    fn from(l: &ffi::FmUnsupportedFabricPartitionList) -> Self {
        Self {
            version: l.version,
            num_partitions: l.num_partitions,
            partition_info: l.partition_info[..l.num_partitions as usize]
                .iter()
                .map(UnsupportedPartitionInfo::from)
                .collect(),
        }
    }
}

impl fmt::Display for UnsupportedPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Unsupported Partitions ({}):\n", self.num_partitions)?;
        writeln!(f, "  {:>3}  {:>4}  GPU IDs", "ID", "GPUs")?;
        for p in &self.partition_info {
            let gpu_ids: Vec<String> = p.gpu_physical_ids.iter().map(|id| id.to_string()).collect();
            writeln!(
                f,
                "  {:>3}  {:>4}  {}",
                p.partition_id,
                p.num_gpus,
                gpu_ids.join(",")
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NvlinkFailedDevices {
    pub version: u32,
    pub num_gpus: u32,
    pub num_switches: u32,
    pub gpu_info: Vec<FailedDeviceInfo>,
    pub switch_info: Vec<FailedDeviceInfo>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FailedDeviceInfo {
    pub uuid: String,
    pub pci_bus_id: String,
    pub num_ports: u32,
    pub port_num: Vec<u32>,
}

impl From<&ffi::FmNvlinkFailedDeviceInfo> for FailedDeviceInfo {
    fn from(d: &ffi::FmNvlinkFailedDeviceInfo) -> Self {
        Self {
            uuid: cstr_from_buf(&d.uuid),
            pci_bus_id: cstr_from_buf(&d.pci_bus_id),
            num_ports: d.num_ports,
            port_num: d.port_num[..d.num_ports as usize].to_vec(),
        }
    }
}

impl From<&ffi::FmNvlinkFailedDevices> for NvlinkFailedDevices {
    fn from(d: &ffi::FmNvlinkFailedDevices) -> Self {
        Self {
            version: d.version,
            num_gpus: d.num_gpus,
            num_switches: d.num_switches,
            gpu_info: d.gpu_info[..d.num_gpus as usize]
                .iter()
                .map(FailedDeviceInfo::from)
                .collect(),
            switch_info: d.switch_info[..d.num_switches as usize]
                .iter()
                .map(FailedDeviceInfo::from)
                .collect(),
        }
    }
}

impl fmt::Display for NvlinkFailedDevices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let print_devices = |f: &mut fmt::Formatter<'_>,
                             label: &str,
                             devices: &[FailedDeviceInfo]|
         -> fmt::Result {
            if devices.is_empty() {
                return writeln!(f, "{label}: none");
            }
            writeln!(f, "{label} ({}):\n", devices.len())?;
            writeln!(f, "  {:>32}  {:>16}  Failed Ports", "UUID", "PCI Bus ID")?;
            for d in devices {
                let ports: Vec<String> = d.port_num.iter().map(|p| p.to_string()).collect();
                writeln!(
                    f,
                    "  {:>32}  {:>16}  {}",
                    d.uuid,
                    d.pci_bus_id,
                    ports.join(",")
                )?;
            }
            Ok(())
        };
        print_devices(f, "Failed GPUs", &self.gpu_info)?;
        writeln!(f)?;
        print_devices(f, "Failed NVSwitches", &self.switch_info)
    }
}
