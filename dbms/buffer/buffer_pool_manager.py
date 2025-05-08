import threading
from dbms.buffer.replacer import Replacer
from dbms.common.config import INVALID_PAGE_ID
from dbms.storage.disk_manager import DiskManager
from dbms.storage.page import Page


class BufferPoolManager:
    """
    Manages a buffer pool of pages in memory.
    It is responsible for fetching pages from disk, unpinning pages,
    flushing pages to disk, and creating new pages.
    """

    def __init__(self, pool_size: int, disk_manager: DiskManager, replacer: Replacer):
        self._pool_size = pool_size
        self._disk_manager: DiskManager = disk_manager
        self._replacer: Replacer = replacer

        self._pages: list[Page | None] = [None] * pool_size
        self._page_table: dict[int, int] = {}  # page_id -> frame_id
        self._free_list: list[int] = list(
            range(pool_size))  # list of available frame_ids
        self._lock = threading.Lock()

        # Initialize pages in the pool
        for i in range(pool_size):
            self._pages[i] = Page()  # Create Page objects for each frame

    def _find_free_frame(self) -> int | None:
        """
        Helper to find a free frame.
        Checks free_list first, then asks replacer for a victim.
        If a victim is chosen and is dirty, it's written to disk.
        The victim page's metadata in the page_table is cleared.
        Returns frame_id or None if no frame is available.
        """
        if self._free_list:
            frame_id = self._free_list.pop(0)
            # The page in this frame might have old data if it was previously used
            # and then became free (e.g., after DeletePage).
            # Ensure it's properly reset before use by caller.
            # self._pages[frame_id].reset_memory() # Caller (fetch_page/new_page) will set page_id etc.
            return frame_id
        victim_frame_id = self._replacer.victim()
        if victim_frame_id is not None:
            victim_page = self._pages[victim_frame_id]
            assert victim_page is not None, "Victim page should exist"

            if victim_page.is_dirty:
                self._disk_manager.write_page_data(
                    victim_page.page_id, victim_page.data)
                victim_page.mark_clean()  # No longer dirty after writing

            # Remove old page from page_table
            if victim_page.page_id != INVALID_PAGE_ID:  # Check if it was a valid page
                if victim_page.page_id in self._page_table and self._page_table[victim_page.page_id] == victim_frame_id:
                    del self._page_table[victim_page.page_id]

            # The page object itself is reset by the caller when new data is loaded.
            # victim_page.reset_memory()
            # victim_page.page_id = INVALID_PAGE_ID
            return victim_frame_id

        return None

    def fetch_page(self, page_id: int) -> Page | None:
        """
        Fetches the requested page from the buffer pool.
        1. If page is already in buffer pool, return it.
        2. If not, find a replacement frame (from free list or replacer).
        3. If replacement frame found, read page from disk and return it.
        4. If no replacement frame available, return None.
        """
        with self._lock:
            if page_id in self._page_table:  # Page already in buffer pool
                frame_id = self._page_table[page_id]
                page = self._pages[frame_id]
                assert page is not None
                page.increment_pin_count()
                self._replacer.pin(frame_id=frame_id)

            # Page not in buffer pool, need to find a frame for it
            frame_id = self._find_free_frame()
            if frame_id is None:  # No free frame or victim available
                return None

            # Found a frame, now load the page data
            target_page = self._pages[frame_id]
            assert target_page is not None

            # Reset the page object for new
            target_page.reset_memory()  # Clear old data and metadata
            target_page.page_id = page_id  # Set new page_id

            self._disk_manager.read_page_data(page_id, target_page.data)
            target_page.increment_pin_count()  # Pin count becomes 1
            target_page.mark_clean()  # Freshly read from disk, so it's clean

            self._page_table[page_id] = frame_id
            self._replacer.pin(frame_id)  # Pin the newly fetched page's frame

            return target_page

    def new_page(self) -> Page | None:
        """
        Creates a new page.
        1. Find a replacement frame.
        2. If found, allocate a new page_id from disk_manager.
        3. Initialize the page, add to page_table, and return it.
           The new page is initially written to disk as empty/zeroed to ensure persistence.
        4. If no replacement frame available, return None.
        """
        with self._lock:
            frame_id = self._find_free_frame()
            if frame_id is None:
                return None

            new_page_id = self._disk_manager.allocate_page()
            if new_page_id == INVALID_PAGE_ID:  # Should not happen with current disk_manager
                # Potentially, if disk_manager ran out of IDs or space, but not implemented that way yet.
                # If _find_free_frame returned a victim, we need to put it back to replacer
                # TODO: if we can't complete new_page. This logic can get complex.
                # For now, assume allocate_page succeeds.
                return None

            target_page = self._page[frame_id]
            target_page.page_id = new_page_id
            target_page.increment_pin_count()  # Pin count is 1
            # New pages are often considered dirty immediately because they need to be written
            # to disk to "exist" there, even if content is just zeros.
            # Or, DiskManager's allocate_page could pre-allocate space and zero it out.
            # Let's assume we write it out now to make it durable.
            self._disk_manager.write_page_data(
                new_page_id, target_page.data)  # Write zeroed page to disk
            # It's now clean as it matches disk. If user modifies, it'll become dirty.
            target_page.mark_clean()

            self._page_table[new_page_id] = frame_id
            self._replacer.pin(frame_id)

            return target_page

    def unpin_page(self, page_id: int, is_dirty: bool) -> bool:
        """
        Unpins a page from the buffer pool.
        If is_dirty is True, the page is marked as dirty.
        If pin_count becomes 0, the page is added to the replacer.
        """
        with self._lock:
            if page_id not in self._page_table:
                return False  # Page not found

            frame_id = self._page_table[page_id]
            page = self._pages[frame_id]
            assert page is not None

            if page.pin_count <= 0:  # Cannot unpin a page with pin_count <= 0
                return False

            page.decrement_pin_count()
            if is_dirty:
                page.mark_dirty()

            if page.pin_count == 0:
                self._replacer.unpin(frame_id)  # Now eligible for replacement

            return True

    def flush_page(self, page_id: int) -> bool:
        """
        Flushes a specific page from the buffer pool to disk if it's dirty.
        """
        with self._lock:
            if page_id not in self._page_table:
                return False  # Page not found

            frame_id = self._page_table[page_id]
            page = self._pages[frame_id]
            assert page is not None

            # No need to check if dirty here, disk_manager.write_page_data is called regardless
            # The page itself tracks dirty status. If it's actually written, mark clean.
            self._disk_manager.write_page_data(page.page_id, page.data)
            page.mark_clean()  # After flushing, it's no longer dirty
            return True

    def flush_all_pages(self) -> None:
        """Flushes all dirty pages in the buffer pool to disk."""
        with self._lock:
            # list() to avoid issues if table changes
            for page_id in list(self._page_table.keys()):
                # Check if page_id is still validly in table, could be removed by another thread
                if page_id in self._page_table:
                    frame_id = self._page_table[page_id]
                    page = self._pages[frame_id]
                    if page is not None and page.is_dirty:  # Only flush if dirty
                        self._disk_manager.write_page_data(
                            page.page_id, page.data)
                        self.mark_clean()

    def delete_page(self, page_id: int) -> bool:
        """
        Deletes a page from the buffer pool and requests deallocation from disk_manager.
        The page must not be pinned.
        """
        with self._lock:
            if page_id not in self._page_table:
                # Page not in buffer pool. but still try to deallocate from disk
                self._disk_manager.deallocate_page(page_id)
                # Or false if we only consider it successful if it was in buffer.
                return True

            frame_id = self._page_table[page_id]
            page = self._pages[frame_id]
            assert page is not None

            if page.pin_count > 0:
                return False  # Cannot delete a pinned page

            # Remove from page_table and replacer, add frame to free_list
            del self._page_table[page_id]
            # Remove from replacer's consideration as it's now free
            self._replacer.pin(frame_id)
            # Pin is used here because it effectively removes from candidates.
            self._free_list.append(frame_id)

            # Reset the page object in the frame
            page.reset_memory()
            page.page_id = INVALID_PAGE_ID

            # Deallocate from disk
            self._disk_manager.deallocate_page(page_id)
            return True

    def get_pool_size(self) -> int:
        return self._pool_size

    def get_pages(self) -> list[Page | None]:  # For debugging/testing
        return self._pages

    def get_page_table(self) -> dict[int, int]:  # For debugging/testing
        return self._page_table

    def get_free_list(self) -> list[int]:
        return self._free_list

    def shutdown(self) -> None:
        """Flushes all dirty pages to disk before shutting down."""
        print("BufferPoolManager shutting down. Flushing all pages.")
        self.flush_all_pages()
        # Note: DiskManager shutdown (closing the file) should be handled separately by the top-level application
