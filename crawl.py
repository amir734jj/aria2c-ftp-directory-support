#!/usr/bin/env python3
import paramiko, os, stat, subprocess, argparse, signal, sys, ftplib, time, threading
from concurrent.futures import ThreadPoolExecutor

subprocesses = []

class DownloadProgressTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.total_queued = 0
        self.completed = 0
        self.active_downloads = 0

    def add_task(self):
        with self.lock:
            self.total_queued += 1

    def start_download(self, filename):
        with self.lock:
            self.active_downloads += 1
            print(f"[DOWNLOADING {self.active_downloads} Active] ({self.completed}/{self.total_queued}) -> {filename}")

    def finish_download(self, filename, success=True):
        with self.lock:
            self.active_downloads -= 1
            if success:
                self.completed += 1
                status = "SUCCESS"
            else:
                status = "FAILED"
            print(f"[{status}] ({self.completed}/{self.total_queued}) Finished: {filename}")

    def skip_file(self, filename, reason):
        with self.lock:
            self.completed += 1
            print(f"[SKIPPED] ({self.completed}/{self.total_queued}) {filename} ({reason})")

    def log_line(self, message):
        with self.lock:
            print(message)

tracker = DownloadProgressTracker()

def stop_all_subprocesses():
    print("Stopping all subprocesses...")
    for proc in subprocesses:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"Force stopping aria2c process group {proc.pid}...")
                os.killpg(proc.pid, signal.SIGKILL)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"aria2c process group {proc.pid} is blocked in I/O.")
    print("All subprocesses terminated.")

def signal_handler(sig, frame):
    print("Signal received, stopping...")
    stop_all_subprocesses()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def delete_sftp_file(sftp, path):
    try:
        print(f"Attempting to delete SFTP file: {path}")
        sftp.remove(path)
        print(f"Deleted: {path}")
        dir_path = os.path.dirname(path)
        try:
            if len(sftp.listdir(dir_path)) == 0:
                sftp.rmdir(dir_path)
                print(f"Deleted empty directory: {dir_path}")
        except IOError:
            pass
    except Exception as e:
        print(f"Error deleting SFTP path {path}: {e}")

def create_ftp_connection(host, port, user, password):
    ftp = ftplib.FTP()
    ftp.connect(host, port, timeout=60)
    ftp.login(user, password)
    ftp.set_pasv(True)
    return ftp

def delete_ftp_file(host, port, user, password, path):
    """Creates an independent FTP connection for safe thread execution."""
    try:
        ftp = create_ftp_connection(host, port, user, password)
        print(f"Attempting to delete remote FTP file: {path}")
        ftp.delete(path)
        print(f"Deleted: {path}")

        dir_path = '/'.join(path.strip('/').split('/')[:-1])
        if dir_path:
            try:
                if len(ftp.nlst(dir_path)) == 0:
                    ftp.rmd(dir_path)
                    print(f"Deleted empty directory: {dir_path}")
            except ftplib.error_perm:
                pass
        ftp.quit()
    except Exception as e:
        print(f"Error deleting FTP path {path}: {e}")

def download_file(protocol, remote_path, local_dir, item_filename, item_size, user, password, host, port, max_connections, force, filter_extension):
    if filter_extension and not any(item_filename.endswith(ext.strip()) for ext in filter_extension.split(',')):
        tracker.skip_file(item_filename, f"does not match {filter_extension}")
        return remote_path

    local_file_path = os.path.join(local_dir, item_filename)

    if os.path.exists(local_file_path):
        local_file_size = os.path.getsize(local_file_path)
        if local_file_size == item_size and not force:
            tracker.skip_file(item_filename, "file exists with matching size")
            return remote_path

    remote_url = f"{protocol}://{host}:{port}{remote_path}"

    aria2c_command = [
        "aria2c",
        "--summary-interval=3",             # Emit a summary line every 3 seconds
        "--download-result=hide",
        "--file-allocation=none",
        "--console-log-level=warn",
        f"--ftp-user={user}",
        f"--ftp-passwd={password}",
        remote_url,
        f"-x{max_connections}",
        "-d", local_dir,
        "-o", item_filename,
    ]

    tracker.start_download(item_filename)
    
    process = subprocess.Popen(
        aria2c_command,
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    subprocesses.append(process)

    # Continuously stream and parse aria2c stdout line-by-line
    if process.stdout:
        for line in iter(process.stdout.readline, ''):
            line = line.strip()
            # Match progress lines containing status tags or speed readouts
            if line.startswith("[#") or "CN:" in line or "%" in line:
                tracker.log_line(f"[{item_filename}] {line}")

    process.wait()

    if process in subprocesses:
        subprocesses.remove(process)

    success = process.returncode == 0
    tracker.finish_download(item_filename, success=success)

    return remote_path if success else None

def is_ftp_dir(ftp, path):
    current = ftp.pwd()
    try:
        ftp.cwd(path)
        ftp.cwd(current)
        return True
    except ftplib.error_perm:
        return False

def ftp_recursive_download(ftp, remote_dir, local_dir, user, password, host, port, max_connections, executor, force, filter_extension, cleanup_remote):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    try:
        ftp.cwd(remote_dir)
    except ftplib.error_perm as e:
        print(f"Cannot access remote directory {remote_dir}: {e}")
        return

    items = []
    try:
        for name, metadata in ftp.mlsd():
            if name in ['.', '..']:
                continue
            items.append((name, metadata))
    except (ftplib.error_perm, ConnectionRefusedError, OSError, ftplib.error_temp) as e:
        print(f"MLSD failed ({e}), falling back to NLST...")
        try:
            file_list = ftp.nlst(remote_dir)
            for file_path in file_list:
                name = os.path.basename(file_path)
                if name in ['.', '..'] or not name:
                    continue
                
                full_remote_path = os.path.join(remote_dir, name)
                is_dir = is_ftp_dir(ftp, full_remote_path)
                
                size = 0
                if not is_dir:
                    try:
                        size = ftp.size(full_remote_path) or 0
                    except Exception:
                        size = 0

                metadata = {'type': 'dir' if is_dir else 'file', 'size': size}
                items.append((name, metadata))
        except Exception as e2:
            print(f"Directory listing failed for {remote_dir}: {e2}")
            return

    for name, metadata in items:
        remote_path = os.path.join(remote_dir, name)
        local_path = os.path.join(local_dir, name)

        if metadata.get('type') == 'dir':
            print(f"Entering directory: {remote_path}")
            ftp_recursive_download(ftp, remote_path, local_path, user, password, host, port, max_connections, executor, force, filter_extension, cleanup_remote)
        else:
            size = int(metadata.get('size', 0))
            tracker.add_task()
            future = executor.submit(download_file, "ftp", remote_path, local_dir, name, size, user, password, host, port, max_connections, force, filter_extension)
            
            if cleanup_remote:
                def handle_cleanup(f, path=remote_path):
                    if f.result():
                        delete_ftp_file(host, port, user, password, path)
                future.add_done_callback(handle_cleanup)

def sftp_recursive_download(sftp, remote_dir, local_dir, user, password, host, port, max_connections, executor, force, filter_extension, cleanup_remote):
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    for item in sftp.listdir_attr(remote_dir):
        remote_path = os.path.join(remote_dir, item.filename)
        local_path = os.path.join(local_dir, item.filename)

        if stat.S_ISDIR(item.st_mode):
            print(f"Entering directory: {remote_path}")
            sftp_recursive_download(sftp, remote_path, local_path, user, password, host, port, max_connections, executor, force, filter_extension, cleanup_remote)
        else:
            tracker.add_task()
            future = executor.submit(download_file, "sftp", remote_path, local_dir, item.filename, item.st_size, user, password, host, port, max_connections, force, filter_extension)
            if cleanup_remote:
                future.add_done_callback(lambda f, path=remote_path: delete_sftp_file(sftp, path) if f.result() else None)

def main():
    parser = argparse.ArgumentParser(description="FTP/SFTP recursive downloader.")
    parser.add_argument("--protocol", choices=["ftp", "sftp"], required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--remote-dir", default="/")
    parser.add_argument("--local-dir", default="./downloads")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-concurrency", type=int, default=4)
    parser.add_argument("--max-connections", type=int, default=4)
    parser.add_argument("--filter-extension", default="")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--watch-interval", type=int, default=30)
    parser.add_argument("--cleanup-remote", action="store_true")
    args = parser.parse_args()

    if not args.port:
        args.port = 22 if args.protocol == "sftp" else 21

    if not os.path.exists(args.local_dir):
        os.makedirs(args.local_dir)

    if args.protocol == "sftp":
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            print("Connecting to SFTP server...")
            ssh.connect(args.host, port=args.port, username=args.user, password=args.password,
                        look_for_keys=False, allow_agent=False,
                        disabled_algorithms={"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]})
            sftp = ssh.open_sftp()
            while True:
                print(f"Scanning {args.remote_dir}...")
                with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
                    sftp_recursive_download(sftp, args.remote_dir, args.local_dir, args.user, args.password, args.host, args.port, args.max_connections, executor, args.force, args.filter_extension, args.cleanup_remote)
                if args.watch:
                    print(f"Watching... (retrying in {args.watch_interval} seconds)")
                    time.sleep(args.watch_interval)
                else:
                    break
            sftp.close()
        except Exception as e:
            print(f"An error occurred: {e}")
            stop_all_subprocesses()
            raise
        finally:
            ssh.close()

    elif args.protocol == "ftp":
        try:
            while True:
                print("Connecting to FTP server...")
                ftp = create_ftp_connection(args.host, args.port, args.user, args.password)
                print(f"Scanning {args.remote_dir}...")
                
                with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
                    ftp_recursive_download(ftp, args.remote_dir, args.local_dir, args.user, args.password, args.host, args.port, args.max_connections, executor, args.force, args.filter_extension, args.cleanup_remote)

                try:
                    ftp.quit()
                except Exception:
                    pass

                if args.watch:
                    print(f"Watching... (retrying in {args.watch_interval} seconds)")
                    time.sleep(args.watch_interval)
                else:
                    break
        except Exception as e:
            print(f"An error occurred: {e}")
            stop_all_subprocesses()
            raise

    print("All downloads complete.")

if __name__ == "__main__":
    main()