import os
import threading
from dbms.common.config import PAGE_SIZE, INVALID_PAGE_ID


class DiskManager:
    """
    Manages the database file on disk, allowing reading and writing of pages.
    """

    def __init__(self, db_filepath: str):
        self._db_filepath: str = db_filepath
        self._file_io = None
        self._num_pages: int = 0  # Total number of pages currently in the db file
        self._next_page_id_counter: int = 0  # Counter for allocating new page IDs
        # To protect shared resources like file access and counters
        self._lock = threading.Lock()

        try:
            # Open the file in binary read/write mode.
            # 'r+b' opens for reading and writing; file must exist.
            # 'w+b' creates if not exists, truncates if exists.
            # 'a+b' opens for reading and appending; creates if not exists.
            # We want to open an existing file or create a new one without truncating.
            # So, we'll try 'r+b' and if it fails (FileNotFoundError), we use 'w+b'.
            try:
                self._file_io = open(self._db_filepath, 'r+b')
                print(f"Opened existing database file: {self._db_filepath}")
            except FileNotFoundError:
                self._file_io = open(self._db_filepath, 'w+b')
                print(f"Created new database file: {self._db_file_path}")

            # Determine the number of pages and the next page ID from file
            self._file_io.seek(0, os.SEEK_END)
            file_size = self._file_io.tell()
            self._num_pages = file_size // PAGE_SIZE
            self._next_page_id_counter = self._num_pages
            print(
                f"DB file size: {file_size} bytes, Num pages: {self._num_pages}, Next Page ID: {self._next_page_id_counter}")

        except IOError as e:
            print(
                f"Failed to open or create database file '{self._db_file_path}': {e}")
            # Handle error appropriately, maybe raise an exception to stop DB initialization
            raise SystemExit(f"DiskManager initialization failed: {e}")

    def read_page_data(self, page_id: int, destination_buffer: bytearray) -> None:
        """
        Reads the content of a specific page from the database file into the destination_buffer.
        The destination_buffer must be pre-allocated (e.g., page.data).

        Args:
            page_id: The ID of the page to read.
            destination_buffer: The bytearray (typically a Page's data buffer) to fill.

        Raises:
            ValueError: If page_id is invalid or out of bounds.
            IOError: If a disk I/O error occurs.
        """
        if page_id < 0:  # self._next_page_id_counter handles upper bound implicitly with file growth
            raise ValueError(f"Invalid page_id {page_id}: cannot be negative.")

        if len(destination_buffer) != PAGE_SIZE:
            raise ValueError(
                f"Destination buffer size {len(destination_buffer)} does not match PAGE_SIZE {PAGE_SIZE}.")

        offset = page_id * PAGE_SIZE

        with self._lock:
            if not self._file_io or self._file_io.closed:
                raise IOError("Database file is not open.")

            # Check if the page_id is trying to read beyond the current allocated pages
            # (which might indicate an unallocated page if not careful with allocate_page logic)
            # However, for read, it should strictly be within _num_pages
            if page_id >= self._num_pages:
                # This means we are trying to read a page that hasn't been "officially" written to cover its full extent yet
                # or a page that simply doesn't exist.
                # For a newly allocated page, its contents are undefined until first write.
                # We can choose to fill destination_buffer with zeros or raise error.
                # Raising an error is safer if we expect reads only from "committed" pages.
                # Or, if allocate_page is guaranteed to extend the file with zeros, this is okay.
                # For now, let's assume a read is for an existing page.
                print(
                    f"Warning: Attempting to read page_id {page_id} which might be beyond current actual file content ({self._num_pages} pages). Filling with zeros.")
                destination_buffer[:] = b'\0' * PAGE_SIZE  # Fill with zeros
                return

            self._file_io.seek(offset)
            bytes_read = self._file_io.readinto(destination_buffer)

            if bytes_read != PAGE_SIZE:
                # This could happen if we read the last page and it's smaller than PAGE_SIZE (corrupted file?)
                # Or if we try to read past EOF for some reason (though seek should handle some of this).
                # For simplicity, we might fill the rest of the buffer with zeros or raise error.
                print(
                    f"Warning: Read {bytes_read} bytes for page {page_id}, expected {PAGE_SIZE}. Filling rest with zeros.")
                destination_buffer[bytes_read:] = b'\0' * \
                    (PAGE_SIZE - bytes_read)
                # raise IOError(f"Error reading page {page_id}: read {bytes_read} bytes, expected {PAGE_SIZE}.")

    def write_page_data(self, page_id: int, source_buffer: bytes | bytearray) -> None:
        """
        Writes the content of the source_buffer to a specific page in the database file.
        The source_buffer should contain PAGE_SIZE bytes.

        Args:
            page_id: The ID of the page to write.
            source_buffer: The bytes or bytearray (typically a Page's data buffer) to write.

        Raises:
            ValueError: If page_id is invalid or source_buffer size is incorrect.
            IOError: If a disk I/O error occurs.
        """
        if page_id < 0:
            raise ValueError(f"Invalid page_id {page_id}: cannot be negative.")
        if len(source_buffer) != PAGE_SIZE:
            raise ValueError(
                f"Source buffer size {len(source_buffer)} does not match PAGE_SIZE {PAGE_SIZE}.")

        offset = page_id * PAGE_SIZE

        with self._lock:
            if not self._file_io or self._file_io.closed:
                raise IOError("Database file is not open.")

            self._file_io.seek(offset)
            self._file_io.write(source_buffer)
            self._file_io.flush()  # Ensure data is written to OS buffer

            # Update _num_pages if we wrote to a new page that extends the file
            if page_id >= self._num_pages:
                self._num_pages = page_id + 1
                # Also ensure next_page_id_counter is at least num_pages
                if self._next_page_id_counter < self._num_pages:
                    self._next_page_id_counter = self._num_pages

    def allocate_page(self) -> int:
        """
        Allocates a new page ID. This conceptually reserves space for a new page.
        The actual file extension happens when this new page is first written.

        Returns:
            The ID of the newly allocated page.
        """
        with self._lock:
            new_page_id = self._next_page_id_counter
            self._next_page_id_counter += 1
            # The _num_pages will be updated when write_page_data is called for this new_page_id
            print(f"Allocated new page_id: {new_page_id}")
            return new_page_id

    def deallocate_page(self, page_id: int) -> None:
        """
        Deallocates a page. (Placeholder - complex to implement free space management).
        For now, this method might not do much other than logging.
        True deallocation would require managing a free list or compacting the file.
        """
        # In a real system, this would add page_id to a free list or mark it as reusable.
        # For now, we'll just print a message. This makes allocated pages permanent.
        print(
            f"Warning: Deallocate_page({page_id}) called. Not fully implemented (no free space management).")
        # To prevent re-allocation of this ID by simple counter increment, we don't touch _next_page_id_counter.
        # If we had a free list, allocate_page would first check the free list.

    def get_num_pages(self) -> int:
        """Returns the current number of pages that are considered part of the database file."""
        with self._lock:
            return self._num_pages

    def shutdown(self) -> None:
        """Closes the database file."""
        with self._lock:
            if self._file_io and not self._file_io.closed:
                try:
                    self._file_io.flush()
                    self._file_io.close()
                    print(f"Database file '{self._db_file_path}' closed.")
                    self._file_io = None
                except IOError as e:
                    print(
                        f"Error closing database file '{self._db_file_path}': {e}")
            else:
                print("Database file was not open or already closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
