//! Loaded-image evidence. All calls perform blocking metadata work.
use std::{
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
};

use super::RunningImage as CapturedImage;

#[derive(Clone, Copy, Debug)]
pub(crate) struct LoadedImage {
    pub(crate) device: u64,
    pub(crate) inode: u64,
}

struct RegionEvidence {
    returned_bytes: usize,
    address: u64,
    size: u64,
    offset: u64,
    readable: bool,
    submap: bool,
    device: u64,
    inode: u64,
    mode: u32,
    owner: u32,
}

fn validate_region(
    region: &RegionEvidence,
    header: u64,
    expected_bytes: usize,
) -> io::Result<LoadedImage> {
    let end = region.address.checked_add(region.size);
    let header_end = header.checked_add(32);
    if header == 0
        || expected_bytes == 0
        || region.returned_bytes != expected_bytes
        || region.address != header
        || region.offset != 0
        || end.is_none()
        || header_end.is_none()
        || end < header_end
        || !region.readable
        || region.submap
        || region.device == 0
        || region.inode == 0
        || region.mode & libc::S_IFMT as u32 != libc::S_IFREG as u32
        || region.mode & 0o7022 != 0
        || region.mode & 0o100 == 0
        || region.owner != unsafe { libc::geteuid() }
    {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    Ok(LoadedImage {
        device: region.device,
        inode: region.inode,
    })
}

pub(crate) fn capture_current() -> io::Result<CapturedImage> {
    let installed = std::env::current_exe()?;
    let loaded = loaded_main_image()?;
    capture_installed(&installed, loaded)
}

pub(crate) fn capture_installed(path: &Path, loaded: LoadedImage) -> io::Result<CapturedImage> {
    if !path.is_absolute() {
        return Err(io::Error::from_raw_os_error(libc::EINVAL));
    }
    let original = std::fs::symlink_metadata(path)?;
    let canonical = std::fs::canonicalize(path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(&canonical)?;
    let opened = file.metadata()?;
    let rebound = std::fs::symlink_metadata(path)?;
    let final_path = std::fs::symlink_metadata(&canonical)?;
    for metadata in [&original, &opened, &rebound, &final_path] {
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o7022 != 0
            || metadata.mode() & 0o100 == 0
            || metadata.dev() != loaded.device
            || metadata.ino() != loaded.inode
            || metadata.mode() != opened.mode()
        {
            return Err(io::Error::from_raw_os_error(libc::ESTALE));
        }
    }
    Ok(CapturedImage {
        path: canonical,
        device: loaded.device,
        inode: loaded.inode,
    })
}

#[cfg(not(target_os = "macos"))]
fn loaded_main_image() -> io::Result<LoadedImage> {
    Err(io::Error::from_raw_os_error(libc::ENOTSUP))
}

#[cfg(target_os = "macos")]
fn loaded_main_image() -> io::Result<LoadedImage> {
    // Apple sys/proc_info.h PROC_PIDREGIONPATHINFO (8). libc already supplies
    // vnode_info_path; the region structure is reproduced with its C layout.
    // https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/proc_info.h
    #[repr(C)]
    struct RegionInfo {
        protection: u32,
        maximum_protection: u32,
        inheritance: u32,
        flags: u32,
        offset: u64,
        counters: [u32; 14],
        address: u64,
        size: u64,
    }
    #[repr(C)]
    struct RegionWithPath {
        region: RegionInfo,
        vnode: libc::vnode_info_path,
    }
    unsafe extern "C" {
        fn _dyld_get_image_header(index: u32) -> *const std::ffi::c_void;
    }
    // The dyld index zero header is the main executable, not a library or a
    // current_exe pathname that an installer could have replaced.
    let header = unsafe { _dyld_get_image_header(0) };
    if header.is_null() || unsafe { header.cast::<u32>().read() } != 0xfeedfacf {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    let mut result = std::mem::MaybeUninit::<RegionWithPath>::zeroed();
    let expected_bytes = std::mem::size_of::<RegionWithPath>();
    let returned = unsafe {
        libc::proc_pidinfo(
            std::process::id() as i32,
            8,
            header as u64,
            result.as_mut_ptr().cast(),
            expected_bytes as i32,
        )
    };
    if returned != expected_bytes as i32 {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    let result = unsafe { result.assume_init() };
    let node = &result.vnode.vip_vi;
    if node.vi_type != 1 {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    let region = RegionEvidence {
        returned_bytes: returned as usize,
        address: result.region.address,
        size: result.region.size,
        offset: result.region.offset,
        readable: result.region.protection & 1 != 0,
        submap: result.region.flags & 1 != 0,
        device: u64::from(node.vi_stat.vst_dev),
        inode: node.vi_stat.vst_ino,
        mode: u32::from(node.vi_stat.vst_mode),
        owner: node.vi_stat.vst_uid,
    };
    validate_region(&region, header as u64, expected_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    fn region() -> RegionEvidence {
        RegionEvidence {
            returned_bytes: 128,
            address: 4096,
            size: 4096,
            offset: 0,
            readable: true,
            submap: false,
            device: 7,
            inode: 9,
            mode: libc::S_IFREG as u32 | 0o755,
            owner: unsafe { libc::geteuid() },
        }
    }

    #[test]
    fn mapped_header_requires_exact_result_and_covering_regular_vnode() {
        let valid = region();
        let loaded = validate_region(&valid, 4096, 128).unwrap();
        assert_eq!((loaded.device, loaded.inode), (7, 9));
        for change in 0..10 {
            let mut bad = region();
            match change {
                0 => bad.returned_bytes = 127,
                1 => bad.address = 4097,
                2 => bad.size = 16,
                3 => bad.size = u64::MAX,
                4 => bad.offset = 1,
                5 => bad.readable = false,
                6 => bad.submap = true,
                7 => bad.inode = 0,
                8 => bad.mode = libc::S_IFDIR as u32 | 0o755,
                _ => bad.owner = unsafe { libc::geteuid() }.wrapping_add(1),
            }
            assert!(validate_region(&bad, 4096, 128).is_err(), "case {change}");
        }
        assert!(validate_region(&region(), 0, 128).is_err());
    }

    #[test]
    fn installed_identity_must_match_loaded_inode_and_preserve_canonical_path() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("worker");
        std::fs::write(&path, b"image").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        let loaded = LoadedImage {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let captured = capture_installed(&path, loaded).unwrap();
        assert_eq!(captured.path, std::fs::canonicalize(&path).unwrap());
        assert_eq!(
            (captured.device, captured.inode),
            (metadata.dev(), metadata.ino())
        );
        assert!(
            capture_installed(
                &path,
                LoadedImage {
                    inode: loaded.inode.wrapping_add(1),
                    ..loaded
                }
            )
            .is_err()
        );
        assert!(
            capture_installed(
                &path,
                LoadedImage {
                    device: loaded.device.wrapping_add(1),
                    ..loaded
                }
            )
            .is_err()
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(capture_installed(&path, loaded).is_err());
    }

    #[test]
    fn replaced_source_cannot_be_adopted_as_the_running_image() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("worker");
        std::fs::write(&path, b"old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let old = std::fs::metadata(&path).unwrap();
        let loaded = LoadedImage {
            device: old.dev(),
            inode: old.ino(),
        };
        capture_installed(&path, loaded).unwrap();
        let new = temp.path().join("new");
        std::fs::write(&new, b"new").unwrap();
        std::fs::set_permissions(&new, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::rename(&new, &path).unwrap();
        assert!(capture_installed(&path, loaded).is_err());
        symlink(&path, temp.path().join("link")).unwrap();
        assert!(capture_installed(&temp.path().join("link"), loaded).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn system_capture_proves_actual_main_image_vnode() {
        let captured = capture_current().unwrap();
        let metadata = std::fs::metadata(&captured.path).unwrap();
        assert_eq!(
            (captured.device, captured.inode),
            (metadata.dev(), metadata.ino())
        );
        assert_eq!(
            captured.path,
            std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap()
        );
    }
}
