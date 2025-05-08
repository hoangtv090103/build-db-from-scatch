import threading
from typing import List
from dbms.buffer.buffer_pool_manager import BufferPoolManager
from dbms.storage.slotted_page import SlottedPageWrapper
from dbms.storage.page import Page
from dbms.common.rid import RID
from dbms.common.config import INVALID_PAGE_ID


class TableIterator:
    def __init__(self, buffer_pool_manager: BufferPoolManager, page_ids: list[int]):
        self.buffer_pool_manager = buffer_pool_manager
        self.page_ids_for_table = page_ids
        self.current_page_idx_in_list = -1  # Increase to 0 when loading the first page
        self.current_page = None
        self.current_slotted_page = None
        self.current_record_idx_on_page = 0
        self.records_on_current_page = []
        self._load_next_page_if_needed()  # Try to load the first page. False if full

    def _load_next_page_if_needed(self) -> bool:
        if self.current_page is not None:
            self.buffer_pool_manager.unpin_page(
                self.current_page.page_id, is_dirty=False)
            self.current_page = None
            self.current_slotted_page = None

            self.current_page_idx_in_list = self.current_page_idx_in_list + 1

        if self.current_page_idx_in_list >= len(self.page_ids_for_table):
            return False  # out of page to iterate

        current_pid_to_load = self.page_ids_for_table[self.current_page_idx_in_list]
        self.current_page = self.buffer_pool_manager.fetch_page(
            current_pid_to_load
        )

        if self.current_page is None:
            self.records_on_current_page = []
            self.current_record_idx_on_page = 0
            # try to load next page if this page error
            return self._load_next_page_if_needed()

        self.current_slotted_page = SlottedPageWrapper(self.current_page)
        self.records_on_current_page = self.current_slotted_page.get_all_valid_rids_and_records()
        self.current_record_idx_on_page = 0
        return True

    def next(self) -> tuple[RID, bytes] | None:
        while True:
            if self.current_slotted_page is None \
                or self.current_record_idx_on_page >= len(self.records_on_current_page) \
                    and not self._load_next_page_if_needed():
                # load next page or all records in current page
                return None  # Out of data
            else:
                slot_num, record_data = self.records_on_current_page[self.current_record_idx_on_page]
                self.current_record_idx_on_page = self.current_record_idx_on_page + 1

                current_actual_page_id = self.current_page.page_id
                rid = RID(current_actual_page_id, slot_num)
                return (rid, record_data)

    def close(self) -> None:
        """close() called when iterator not used anymore to unpin the last page"""
        if self.current_page is not None:
            self.buffer_pool_manager.unpin_page(
                self.current_page.page_id,
                is_dirty=False
            )
            self.current_page = None
            self.current_slotted_page = None


class TableHeap:
    def __init__(self, buffer_pool_manager: BufferPoolManager, table_page_ids: List[int]):
        self.buffer_pool_manager = buffer_pool_manager
        self.table_page_ids = table_page_ids
        self._lock = threading.Lock()

    def insert_record(self, record_data: bytes) -> RID | None:
        with self._lock:
            for page_id in sorted(self.table_page_ids, reverse=True):
                page = self.buffer_pool_manager.fetch_page(page_id)
                if page is None:
                    continue  # Cannot fetch page, try another one

                slotted_page = SlottedPageWrapper(page)
                slot_num = slotted_page.insert_record(record_data)

                if slot_num is not None:  # Insert into this page successfully
                    self.buffer_pool_manager.unpin_page(page_id, is_dirty=True)
                    return RID(page_id, slot_num)
                else:
                    # Page is full or error
                    self.buffer_pool_manager.unpin_page(
                        # (SlottedPageWrapper mark dirty automatically if changed unsuccessfully)
                        page_id, is_dirty=slotted_page.page.is_dirty)

            # If there's no suitable page, allocate a new page
            new_page = self.buffer_pool_manager.new_page()
            if new_page is None:
                return None  # Cannot allocate new page

            new_page_id = new_page.page_id
            slotted_new_page = SlottedPageWrapper(new_page)
            slotted_new_page.initialize()  # prepare for new page

            slot_num = slotted_new_page.insert_record(record_data)

            if slot_num is None:  # Insert unsuccessfully
                self.buffer_pool_manager.unpin_page(
                    new_page_id, is_dirty=slotted_new_page.page.is_dirty)
                self.buffer_pool_manager.delete_page(
                    new_page_id)  # Undo allocate page
                return None

            self.table_page_ids.append(new_page_id)
            self.buffer_pool_manager.unpin_page(
                new_page_id, is_dirty=True)
            return RID(new_page_id, slot_num)

    def get_record(self, rid: RID) -> bytes | None:
        """Fetch data of a record bases on RID"""
        with self._lock:
            if not rid.is_valid() or rid.page_id == INVALID_PAGE_ID:
                return None

            page = self.buffer_pool_manager.fetch_page(rid.page_id)
            if page is None:
                return None

            slotter_page = SlottedPageWrapper(page)
            record_data = slotter_page.get_record(rid.slot_num)

            self.buffer_pool_manager.unpin_page(rid.page_id, is_dirty=False)

            return record_data

    def delete_record(self, rid: RID) -> bool:
        with self._lock:
            if not rid.is_valid() or rid.page_id == INVALID_PAGE_ID:
                return False

            page = self.buffer_pool_manager.fetch_page(rid.page_id)
            if page is None:
                return False

            slotted_page = SlottedPageWrapper(page)
            ok = slotted_page.delete_record(rid.slot_num)

            # is_dirty is True if ok and slotted_page.delete_record changed page
            self.buffer_pool_manager.unpin_page(
                rid.page_id, is_dirty=page.is_dirty)

            return ok

    def iterator(self) -> TableIterator:
        """Return a iterator object to iterate through all valid records in table"""
        with self._lock:
            # Need a copy of page_ids for iterator not be affected if table_page_ids changes in TableHeap
            current_table_pages = self.table_page_ids.copy()
            return TableIterator(self.buffer_pool_manager, current_table_pages)
