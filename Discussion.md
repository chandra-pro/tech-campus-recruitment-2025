Solutions Considered
Sequential Scan (Line-by-Line Processing):

Approach: Open the file and iterate line by line, writing out matching entries.
Pros: Simple to implement; low memory footprint.
Cons: Every query requires scanning a significant portion of the file, leading to high latency for large files.
Indexing Only:

Approach: Perform a one-time pass to build an index mapping dates to byte offsets, then seek directly to the desired section for each query.
Pros: Greatly reduces the search space for each query; fast retrieval after the index is built.
Cons: The initial indexing pass can be time-consuming, and seeking in text mode (with UTF‑8) may cause issues with variable-length encoding.
Memory-Mapped Files (mmap):

Approach: Memory-map the file so that the OS handles paging in data, and perform in-memory searches.
Pros: Fast random access; leverages OS-level optimizations.
Cons: More complex to manage with variable-length encodings and ensuring line boundaries.
Binary Search on File (Using the Sorted Nature of Timestamps):

Approach: Exploit the fact that log entries are chronological by performing a binary search to locate the start of a given date.
Pros: Reduces search time to O(log n).
Cons: Implementation complexity increases due to handling partial lines and encoding issues.
Parallel Processing with Indexing:

Approach: Combine an indexing phase (to quickly locate the start offset for a date) with parallel processing that divides the target section into chunks.
Pros: Combines the benefits of fast random access (via the index) with the speed-up provided by multiple cores.
Cons: Requires careful handling of chunk boundaries and ensuring that lines are not split.
Final Solution Summary
The final solution leverages indexing combined with parallel processing. We first build an index that maps each date to its corresponding byte offset in the file. For each query, we then use this index to determine the exact range in the file that contains logs for the target date. That range is divided into chunks that are processed in parallel using Python’s multiprocessing, reading the file in binary mode to ensure accurate byte-level access. This hybrid approach was chosen because:

Speed: The index allows us to bypass unnecessary parts of the file, while parallel processing utilizes multiple CPU cores to further reduce the total processing time.
Memory Efficiency: Only the necessary file portions are loaded at a time, and processing is done in a streaming manner, preventing excessive memory usage.
Scalability: This design scales well even for a 1 TB file, as both the indexing data and per-chunk processing remain small relative to the full file size.
Steps to Run
Download and Prepare the Log File:
Ensure you have the 1 TB log file available on your system. The provided links allow you to download a zipped version—unzip it to get the raw log file.

Build the Index (Optional):
The solution automatically checks for an existing index file (log_index.txt). If it’s not present, the script will build the index.

sh
Copy
Edit
python extract_logs.py 2024-12-01
On the first run, the script will scan the file to create the index and save it in log_index.txt.

Extract Logs for a Specific Date:
Run the script with the log file path and the target date (formatted as YYYY-MM-DD). The script will:

Load or build the index.
Calculate the start and end offsets for the given date.
Divide the target range into chunks and process them in parallel.
Write the output to output/output_YYYY-MM-DD.txt.
sh
Copy
Edit
python extract_logs.py 2024-12-01
Review the Output:
The extracted logs for the specified date will be saved in the output/ directory, named output_YYYY-MM-DD.txt.