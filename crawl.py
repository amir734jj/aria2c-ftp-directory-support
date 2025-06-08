#!/usr/bin/env python3
import paramiko, os, stat, subprocess, argparse, signal, sys, ftplib, time
from concurrent.futures import ThreadPoolExecutor

# List to track subprocesses
subprocesses = []

# Function to stop all subprocesses on error or interrupt
def stop_all_subprocesses():
    print("Stopping all subprocesses...")
    for proc in subprocesses:
        if proc.poll() is None:  # If process is still running
            proc.terminate()
            try:
                proc.wait(timeout=5)  # Give the process some time to terminate
            except subprocess.TimeoutExpired:
                proc.kill()  # Forcefully kill if it doesn't terminate in time
    print("All subprocesses terminated.")

# Signal handler to stop processes gracefully
def signal_handler(sig, frame):
    print("Signal received, stopping...")
    stop_all_subprocesses()
    sys.exit(0)  # Exit the program after stopping subprocesses

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# Function to download a single file using aria2c with extension filtering
def download_file(protocol, remote_path, local_dir, item_filename, item_size, user, password, host, port, max_connections, force, filter_extension):
    print("executing download_file for " + remote_path + filter_extension)
    
    # Check if the file matches the extension filter (if provided)
    if filter_extension and not any(map(lambda x: item_filename.endswith(x), filter_extension.split(','))):
        print(f"Skipping {item_filename}: does not match extension {filter_extension}")
        return

    local_file_path = os.path.join(local_dir, item_filename)

    # Check if the file exists and if its size matches the remote file size
    if os.path.exists(local_file_path):
        local_file_size = os.path.getsize(local_file_path)

        if local_file_size == item_size and not force:
            print(f"Skipping {local_file_path}: file exists with the same size.")
            return
        elif local_file_size != item_size:
            print(f"File {local_file_path} exists but sizes differ: local size {local_file_size}, remote size {item_size}. Re-downloading.")

    remote_url = None
    if protocol == "sftp":
        remote_url = f"sftp://{host}:{port}{remote_path}"
    elif protocol == "ftp":
        remote_url = f"ftp://{host}:{port}{remote_path}"

    aria2c_command = [
        "aria2c",
        "--download-result=hide",
        "--file-allocation=none",
        f"--ftp-user={user}",
        f"--ftp-passwd={password}",
        remote_url,
        f"-x{max_connections}", # Number of connections
        "-d", local_dir,        # Set the download directory
        "-o", item_filename,    # Set the output filename
    ]

    print(f"Starting download: {remote_path} -> {local_file_path}")
    process = subprocess.Popen(aria2c_command)
    subprocesses.append(process)  # Track this subprocess
    process.wait()  # Wait for the process to complete
    return process.returncode  # Return the exit code

# Function to handle FTP directory crawling and file listing
def ftp_recursive_download(ftp, remote_dir, local_dir, user, password, host, port, max_connections, executor, force, filter_extension):
    # Ensure the local directory exists
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # List the contents of the remote directory
    ftp.cwd(remote_dir)
    for item in ftp.mlsd():
        name, metadata = item
        remote_path = os.path.join(remote_dir, name)
        local_path = os.path.join(local_dir, name)

        if metadata['type'] == 'dir':
            # Recursively crawl into the directory
            print(f"Entering directory: {remote_path}")
            ftp_recursive_download(ftp, remote_path, local_path, user, password, host, port, max_connections, executor, force, filter_extension)
        else:
            size = int(metadata['size'])
            executor.submit(download_file, "ftp", remote_path, local_dir, name, size, user, password, host, port, max_connections, force, filter_extension)

# Function to handle SFTP directory crawling and file listing
def sftp_recursive_download(sftp, remote_dir, local_dir, user, password, host, port, max_connections, executor, force, filter_extension):
    # Ensure the local directory exists
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # List the contents of the remote directory
    for item in sftp.listdir_attr(remote_dir):
        remote_path = os.path.join(remote_dir, item.filename)
        local_path = os.path.join(local_dir, item.filename)

        if stat.S_ISDIR(item.st_mode):
            # Recursively crawl into the directory
            print(f"Entering directory: {remote_path}")
            sftp_recursive_download(sftp, remote_path, local_path, user, password, host, port, max_connections, executor, force, filter_extension)
        else:
            # Submit the download task to the executor with file size check
            executor.submit(download_file, "sftp", remote_path, local_dir, item.filename, item.st_size, user, password, host, port, max_connections, force, filter_extension)

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="FTP/SFTP recursive downloader with optional force overwrite.")
    parser.add_argument("--protocol", choices=["ftp", "sftp"], required=True, help="Choose between 'ftp' and 'sftp' protocols.")
    parser.add_argument("--host", required=True, help="Server host.")
    parser.add_argument("--port", type=int, default=None, help="Server port (defaults: 21 for FTP, 22 for SFTP).")
    parser.add_argument("--user", required=True, help="Username.")
    parser.add_argument("--password", required=True, help="Password.")
    parser.add_argument("--remote-dir", default="/", help="Remote directory on the server.")
    parser.add_argument("--local-dir", default="./downloads", help="Local directory to save downloaded files.")
    parser.add_argument("--force", action="store_true", help="Overwrite files even if they exist with the same size.")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Maximum number of concurrent downloads (default: 4).")
    parser.add_argument("--max-connections", type=int, default=4, help="Maximum number of aria2c connections per file (default: 4).")
    parser.add_argument("--filter-extension", default="", help="Download only files with the specified extension.")
    parser.add_argument("--watch", action="store_true", help="Watch the remote directory and recheck periodically when no files match.")
    parser.add_argument("--watch-interval", type=int, default=30, help="Interval in seconds to wait before rechecking in watch mode (default: 30s).")
    args = parser.parse_args()

    # Set default ports if not provided
    if not args.port:
        args.port = 22 if args.protocol == "sftp" else 21

    # Ensure the local base directory exists
    if not os.path.exists(args.local_dir):
        os.makedirs(args.local_dir)

    # Start download process based on the chosen protocol
    if args.protocol == "sftp":
        # Create an SSH client for SFTP
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            print("Connecting to SFTP server...")
            ssh.connect(args.host, port=args.port, username=args.user, password=args.password)

            sftp = ssh.open_sftp()
            while True:
                print(f"Scanning {args.remote_dir}...")
                with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
                    sftp_recursive_download(sftp, args.remote_dir, args.local_dir, args.user, args.password, args.host, args.port, args.max_connections, executor, args.force, args.filter_extension)

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
        # Connect to the FTP server
        try:
            ftp = ftplib.FTP()
            print("Connecting to FTP server...")
            ftp.connect(args.host, args.port, timeout=60) # Set timeout to 60 seconds
            ftp.login(args.user, args.password)

            while True:
                print(f"Scanning {args.remote_dir}...")
                with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
                    ftp_recursive_download(ftp, args.remote_dir, args.local_dir, args.user, args.password, args.host, args.port, args.max_connections, executor, args.force, args.filter_extension)

                if args.watch:
                    print(f"Watching... (retrying in {args.watch_interval} seconds)")
                    time.sleep(args.watch_interval)
                else:
                    break

            try:
                ftp.voidcmd("NOOP")
                ftp.quit()
            except EOFError:
                print("FTP connection was already closed.")
            except Exception as e:
                print(f"An unexpected error occurred during quit: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")
            stop_all_subprocesses()
            raise

    print("All downloads complete.")

if __name__ == "__main__":
    main()
