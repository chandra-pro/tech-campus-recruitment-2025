
import os
import sys
from multiprocessing import Pool, cpu_count

INDEX_FILE = "log_index.txt"
INDEX_INFO_FILE = "index_info.txt"

def build_index(log_file, index_file=INDEX_FILE, index_info_file=INDEX_INFO_FILE):
    """
    Build an index mapping each date (YYYY-MM-DD) to the byte offset where that date first appears.
    Also record the current file size (last processed offset) in index_info_file.
    """
    index = {}
    with open(log_file, "rb") as f:
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                line_str = line.decode("utf-8", errors="ignore")
            except Exception:
                continue
            if len(line_str) < 10:
                continue
            date = line_str[:10]
            if date not in index:
                index[date] = offset

    # Write the index file sorted by date.
    with open(index_file, "w") as idx_f:
        for date in sorted(index.keys()):
            idx_f.write(f"{date} {index[date]}\n")

    # Record the last processed offset (the current file size)
    last_offset = os.path.getsize(log_file)
    with open(index_info_file, "w") as info_f:
        info_f.write(str(last_offset))

    print(f"Index built and saved to {index_file}. Last processed offset: {last_offset}")

def load_index(index_file=INDEX_FILE):
    """
    Load the index file into a dictionary mapping date -> offset.
    """
    index = {}
    if not os.path.exists(index_file):
        return index
    with open(index_file, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                date, offset = parts
                index[date] = int(offset)
    return index

def update_index(log_file, index_file=INDEX_FILE, index_info_file=INDEX_INFO_FILE):
    """
    Update the index file with any new log entries appended to log_file.
    Reads from the last processed offset (stored in index_info_file) to the end.
    If new dates appear in the appended logs, their starting offset is added to the index.
    Finally, updates the last processed offset.
    """
    index = load_index(index_file)
    
    # Get last processed offset from the index info file.
    last_processed_offset = 0
    if os.path.exists(index_info_file):
        with open(index_info_file, "r") as info_f:
            try:
                last_processed_offset = int(info_f.read().strip())
            except Exception:
                last_processed_offset = 0

    current_size = os.path.getsize(log_file)
    if current_size <= last_processed_offset:
        print("No new logs to update.")
        return

    new_entries = {}
    with open(log_file, "rb") as f:
        f.seek(last_processed_offset)
        while True:
            offset = f.tell()
            line = f.readline()
            if not line:
                break
            try:
                line_str = line.decode("utf-8", errors="ignore")
            except Exception:
                continue
            if len(line_str) < 10:
                continue
            date = line_str[:10]
            # If the date is new (not in our current index), record it.
            if date not in index and date not in new_entries:
                new_entries[date] = offset

    # Append any new date entries to the index file.
    if new_entries:
        with open(index_file, "a") as idx_f:
            for date in sorted(new_entries.keys()):
                idx_f.write(f"{date} {new_entries[date]}\n")
                index[date] = new_entries[date]
        print("Index updated with new date entries.")
    else:
        print("No new date entries found in the appended logs.")

    # Update the index info file with the new file size.
    with open(index_info_file, "w") as info_f:
        info_f.write(str(current_size))

def get_date_range(target_date, index, log_file):
    """
    Given a target date and the index (date -> byte offset),
    return the start and end offsets for that date in the log file.
    If no later date exists, end_offset is set to the file size.
    """
    if target_date not in index:
        return None, None

    sorted_dates = sorted(index.keys())
    start_offset = index[target_date]
    # Find the next date after target_date
    next_date = None
    for date in sorted_dates:
        if date > target_date:
            next_date = date
            break
    if next_date:
        end_offset = index[next_date]
    else:
        end_offset = os.path.getsize(log_file)
    return start_offset, end_offset

def process_chunk(args):
    log_file, chunk_start, chunk_end, target_date = args
    output_lines = []
    with open(log_file, "rb") as f:
        f.seek(chunk_start)
        # Discard partial line if not at file start.
        if chunk_start != 0:
            f.readline()
        while True:
            pos = f.tell()
            # Break if we have passed chunk_end and already processed one extra line.
            if pos >= chunk_end:
                # Read one extra line to capture a potential partial entry.
                line = f.readline()
                if not line:
                    break
                try:
                    line_str = line.decode('utf-8', errors='ignore')
                except Exception:
                    break
                if line_str.startswith(target_date):
                    output_lines.append(line_str)
                break
            line = f.readline()
            if not line:
                break
            try:
                line_str = line.decode('utf-8', errors='ignore')
            except Exception:
                continue
            if line_str.startswith(target_date):
                output_lines.append(line_str)
    return output_lines

def extract_logs(log_file, target_date, index_file=INDEX_FILE, output_dir="../output"):
    """
    Extract logs for a given date using the prebuilt index and parallel processing.
    The result is saved to output/output_<target_date>.txt.
    """
    # Ensure the index exists.
    if not os.path.exists(index_file):
        print("Index file not found; building index...")
        build_index(log_file, index_file)
      # First, build the full index (if not already built).
    else:
        # Then, update the index if new logs have been appended.
        update_index(log_file,index_file)

    index = load_index(index_file)
    if target_date not in index:
        print(f"Target date {target_date} not found in the index!")
        return
    else:
        print(f"Offset for {target_date}: {index[target_date]}")
        start_offset, end_offset = get_date_range(target_date, index, log_file)
        if start_offset is None:
            print(f"No logs found for date {target_date}.")
            return

    # Determine number of workers (using available CPU cores or a fixed number).
    num_workers = 4

    total_bytes = abs(end_offset - start_offset)
    print(end_offset)
    print(start_offset)
    if total_bytes <= 0:
        print("No data in the specified range.")
        return

    chunk_size = total_bytes // num_workers

    tasks = []
    for i in range(num_workers):
        chunk_start = start_offset + i * chunk_size
        # The last chunk goes to end_offset.
        if i == num_workers - 1:
            chunk_end = end_offset
        else:
            chunk_end = start_offset + (i + 1) * chunk_size
        tasks.append((log_file, chunk_start, chunk_end, target_date))

    # Use multiprocessing to process chunks in parallel.
    with Pool(num_workers) as pool:
        results = pool.map(process_chunk, tasks)

    # Flatten results and write to output file.
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = os.path.join(output_dir, f"output_{target_date}.txt")
    with open(output_file, "w", encoding="utf-8") as out_f:
        for chunk in results:
            out_f.writelines(chunk)

    print(f"Logs for {target_date} saved to {output_file}")





if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python extract_logs.py <log_file> <YYYY-MM-DD>")
        sys.exit(1)

    log_file = "logs_2024.log"
    target_date = sys.argv[1]
    print(log_file)
    print(target_date)

    extract_logs(log_file, target_date)