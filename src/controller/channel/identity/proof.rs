//! Bounded existing-listener hello using the frozen codec seam, never a sibling connector.
use super::super::contracts::{ChannelCodec,ChannelFailure,ChannelReason,ClientContext,SocketIdentity,IDENTITY_BYTES,SETUP_GUARD};
use std::{io::{self,Read,Write},os::{fd::{AsRawFd,FromRawFd},unix::net::UnixStream},path::Path,time::Duration};
fn unavailable()->ChannelFailure {ChannelFailure::Unavailable(ChannelReason::ServiceUnavailable)}
fn check(ctx:&ClientContext<'_>,deadline:Duration)->Result<(),ChannelFailure> {
    ctx.check()?;
    if ctx.runtime.now()>=deadline {return Err(ChannelFailure::Unavailable(ChannelReason::Timeout));}
    Ok(())
}
fn ready(stream:&UnixStream,event:i16,ctx:&ClientContext<'_>,deadline:Duration)->Result<(),ChannelFailure> {
    loop {
        check(ctx,deadline)?;
        let remaining=deadline.saturating_sub(ctx.runtime.now()).min(Duration::from_millis(50));
        let mut fd=libc::pollfd{fd:stream.as_raw_fd(),events:event,revents:0};
        // SAFETY: fd refers to a live socket; poll borrows a single initialized entry.
        let result=unsafe {libc::poll(&mut fd,1,remaining.as_millis().max(1) as i32)};
        check(ctx,deadline)?;
        if result>0 {
            if fd.revents&(libc::POLLERR|libc::POLLNVAL)!=0 {return Err(unavailable());}
            if fd.revents&(event|libc::POLLHUP)!=0 {return Ok(());}
        } else if result<0 && io::Error::last_os_error().kind()!=io::ErrorKind::Interrupted {return Err(unavailable());}
    }
}
pub(super) fn hello(path:&Path,expected:&SocketIdentity,codec:&dyn ChannelCodec,ctx:&ClientContext<'_>)->Result<(),ChannelFailure> {
    let deadline=ctx.deadline.min(ctx.runtime.now().saturating_add(SETUP_GUARD));
    check(ctx,deadline)?;
    let text=path.to_str().filter(|text|path.is_absolute()&&text.len()<super::super::contracts::SOCKET_PATH_BYTES&&!text.chars().any(|c|c.is_control()||matches!(c,':'|'%'|'$'))).ok_or_else(unavailable)?;
    // SAFETY: socket creates an unaliased descriptor, transferred to stream below.
    let raw=unsafe {libc::socket(libc::AF_UNIX,libc::SOCK_STREAM,0)};
    if raw<0 {return Err(unavailable());}
    let mut stream=unsafe {UnixStream::from_raw_fd(raw)};
    if unsafe {libc::fcntl(raw,libc::F_SETFD,libc::FD_CLOEXEC)}<0 {return Err(unavailable());}
    stream.set_nonblocking(true).map_err(|_|unavailable())?;
    #[cfg(target_os="macos")]
    {
        let enabled:libc::c_int=1;
        if unsafe {libc::setsockopt(raw,libc::SOL_SOCKET,libc::SO_NOSIGPIPE,(&enabled as *const libc::c_int).cast(),std::mem::size_of_val(&enabled) as libc::socklen_t)}!=0 {return Err(unavailable());}
    }
    let mut address:libc::sockaddr_un=unsafe {std::mem::zeroed()};
    address.sun_family=libc::AF_UNIX as libc::sa_family_t;
    for (target,byte) in address.sun_path.iter_mut().zip(text.bytes()) {*target=byte as libc::c_char;}
    let size=std::mem::offset_of!(libc::sockaddr_un,sun_path)+text.len()+1;
    #[cfg(target_os="macos")]
    {address.sun_len=size as u8;}
    check(ctx,deadline)?;
    if unsafe {libc::connect(raw,(&address as *const libc::sockaddr_un).cast(),size as libc::socklen_t)}<0 {
        if !matches!(io::Error::last_os_error().raw_os_error(),Some(libc::EINPROGRESS|libc::EALREADY)) {return Err(unavailable());}
        ready(&stream,libc::POLLOUT,ctx,deadline)?;
        let mut error:libc::c_int=0;
        let mut length=std::mem::size_of_val(&error) as libc::socklen_t;
        if unsafe {libc::getsockopt(raw,libc::SOL_SOCKET,libc::SO_ERROR,(&mut error as *mut libc::c_int).cast(),&mut length)}!=0 || length as usize!=std::mem::size_of_val(&error) || error!=0 {return Err(unavailable());}
    }
    #[cfg(target_os="macos")]
    {
        let (mut uid,mut gid)=(0,0);
        if unsafe {libc::getpeereid(raw,&mut uid,&mut gid)}!=0 || uid!=unsafe {libc::geteuid()} {return Err(unavailable());}
    }
    #[cfg(target_os="linux")]
    {
        let mut peer:libc::ucred=unsafe {std::mem::zeroed()};
        let mut length=std::mem::size_of_val(&peer) as libc::socklen_t;
        if unsafe {libc::getsockopt(raw,libc::SOL_SOCKET,libc::SO_PEERCRED,(&mut peer as *mut libc::ucred).cast(),&mut length)}!=0 || length as usize!=std::mem::size_of_val(&peer) || peer.uid!=unsafe {libc::geteuid()} {return Err(unavailable());}
    }
    let hello=codec.encode_hello(expected)?;
    if hello.len()>IDENTITY_BYTES+4 {return Err(unavailable());}
    let mut sent=0;
    while sent<hello.len() {
        check(ctx,deadline)?;
        match stream.write(&hello[sent..]) {
            Ok(0)=>return Err(unavailable()),
            Ok(n)=>sent+=n,
            Err(error) if error.kind()==io::ErrorKind::WouldBlock=>ready(&stream,libc::POLLOUT,ctx,deadline)?,
            Err(error) if error.kind()==io::ErrorKind::Interrupted=>{},
            Err(_)=>return Err(unavailable()),
        }
    }
    let mut decoder=codec.decoder();
    let mut scratch=[0u8;1024];
    let mut total=0;
    loop {
        check(ctx,deadline)?;
        match stream.read(&mut scratch) {
            Ok(0)=>return Err(unavailable()),
            Ok(n)=>{
                total+=n;
                if total>IDENTITY_BYTES+4 {return Err(unavailable());}
                let progress=decoder.feed(&scratch[..n])?;
                if progress.consumed!=n || decoder.retained_bytes()>IDENTITY_BYTES+4 {return Err(unavailable());}
                if let Some(payload)=progress.payload {
                    if payload.len()>IDENTITY_BYTES {return Err(unavailable());}
                    codec.decode_ready(&payload,expected)?;
                    check(ctx,deadline)?;
                    return Ok(());
                }
            }
            Err(error) if error.kind()==io::ErrorKind::WouldBlock=>ready(&stream,libc::POLLIN,ctx,deadline)?,
            Err(error) if error.kind()==io::ErrorKind::Interrupted=>{},
            Err(_)=>return Err(unavailable()),
        }
    }
}
