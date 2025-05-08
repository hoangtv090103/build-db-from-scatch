import struct
from dbms.storage.page import Page
# , INVALID_PAGE_ID (Page already imports it)
from dbms.common.config import PAGE_SIZE


# Constants for Slotted Page Layout
# These define the structure of the page header within the Page.data bytearray

# Page Header:
# field name              offset  size (bytes)  description
# ----------------------------------------------------------------------------------
# num_records_            0       2             Number of records (slots) currently stored
# free_data_start_ptr_    2       2             Offset from page start to where next record's data block starts (data grows from end of page)
PAGE_HEADER_NUM_RECORDS_OFFSET = 0
PAGE_HEADER_NUM_RECORDS_SIZE = 2  # uint16_t
# Starts right after num_records
PAGE_HEADER_FREE_DATA_START_OFFSET = PAGE_HEADER_NUM_RECORDS_SIZE
PAGE_HEADER_FREE_DATA_START_SIZE = 2  # uint16_t
PAGE_HEADER_SIZE = PAGE_HEADER_NUM_RECORDS_SIZE + \
    PAGE_HEADER_FREE_DATA_START_SIZE  # Total size of fixed header

# Slot Entry in Slot Array:
# field name              offset (within slot) size (bytes)  description
# ----------------------------------------------------------------------------------
# record_offset           0                    2             Offset of the record's data from page start
# record_length           2                    2             Length of the record's data
SLOT_RECORD_OFFSET_OFFSET = 0  # Relative to start of slot entry
SLOT_RECORD_OFFSET_SIZE = 2  # uint16_t
# Starts right after record_offset
SLOT_RECORD_LENGTH_OFFSET = SLOT_RECORD_OFFSET_SIZE
SLOT_RECORD_LENGTH_SIZE = 2  # uint16_t
SLOT_ENTRY_SIZE = SLOT_RECORD_OFFSET_SIZE + \
    SLOT_RECORD_LENGTH_SIZE  # Total size of one slot entry

# Define pack/unpack formats for struct module ('>' for big-endian)
UINT16_FORMAT = '>H'  # Unsigned short (2 bytes)


class SlottedPageWrapper:
    """
    Manages the layout of records within a Page using the Slotted Page technique.
    This wrapper provides methods to interact with records on a given Page object.
    It does not own the Page object, just interprets its data.
    """

    def __init__(self, page: Page):
        self.page = page

    # --- Header Accessor Methods ---
    def get_num_records(self) -> int:
        """Reads the number of records from the page header."""
        return struct.unpack_from(UINT16_FORMAT, self.page.data, PAGE_HEADER_NUM_RECORDS_OFFSET)[0]

    def _set_num_records(self, num_records: int) -> None:
        """Writes the number of records to the page header."""
        struct.pack_into(UINT16_FORMAT, self.page.data,
                         PAGE_HEADER_FREE_DATA_START_OFFSET, num_records)
        self.page.mark_dirty()

    def get_free_data_start_ptr(self) -> int:
        """Reads the free data start pointer from the page header."""
        return struct.unpack_from(UINT16_FORMAT, self.page.data, PAGE_HEADER_FREE_DATA_START_OFFSET)[0]

    def _set_free_data_start_ptr(self, ptr_value: int):
        """Writes the free data start pointer to the page header."""
        struct.pack_into(UINT16_FORMAT, self.page.data,
                         PAGE_HEADER_FREE_DATA_START_OFFSET, ptr_value)
        self.page.mark_dirty()

    # --- Slot Accessor Methods ---
    def _get_slot_offset_on_page(self, slot_num: int) -> int:
        """Calculates the starting offset of a given slot_num on the page."""
        if slot_num < 0 or slot_num >= self.get_num_records():
           # This check is for reads; for inserts, num_records might be the new slot_num
           # Caller must be careful. For get_slot_record_offset/length, this check is appropriate.
            pass  # Allow to proceed for potential new slot write, let insert_record handle bounds

        return PAGE_HEADER_SIZE + (slot_num * SLOT_ENTRY_SIZE)

    def get_slot_record_offset(self, slot_num: int) -> int:
        """Reads the record_offset from the specified slot."""
        num_records = self.get_num_records()
        if not (0 <= slot_num and slot_num < num_records):
            raise IndexError(
                f"Slot number {slot_num} out of range (0-{num_records - 1}).")

        slot_page_offset = self._get_slot_offset_on_page(slot_num)
        return struct.unpack_from(UINT16_FORMAT, self.page.data, slot_page_offset + SLOT_RECORD_OFFSET_OFFSET)[0]

    def _set_slot_record_offset(self, slot_num: int, record_offset: int) -> None:
        """Writes the record_offset to the specified slot."""
        slot_page_offset = self._get_slot_offset_on_page(slot_num)
        struct.pack_into(UINT16_FORMAT, self.page.data,
                         slot_page_offset + SLOT_RECORD_OFFSET_OFFSET)

        self.page.mark_dirty()

    def get_slot_record_length(self, slot_num: int) -> int:
        num_records = self.get_num_records()
        if not (0 <= slot_num and slot_num < num_records):
            raise IndexError(
                f"Slot number {slot_num} out of range (0-{num_records-1}).")
        slot_page_offset = self._get_slot_offset_on_page(slot_num)
        return struct.unpack_from(UINT16_FORMAT, self.page.data, slot_page_offset + SLOT_RECORD_LENGTH_OFFSET)[0]

    def _set_slot_record_length(self, slot_num: int, record_length: int) -> None:
        """Writes the record_length to the specified slot."""
        slot_page_offset = self._get_slot_offset_on_page(slot_num)
        struct.pack_into(UINT16_FORMAT, self.page.data,
                         slot_page_offset + SLOT_RECORD_LENGTH_OFFSET, record_length)
        self.page.mark_dirty()

    # --- Public Interface ---
    def initialize(self) -> None:
        """Initializes a new blank slotted page."""
        self.page.reset_memory()  # Clears data, pin count, dirty flag (but page_id remains)
        self._set_num_records(0)
        # Free data pointer starts at the very end of the page
        self._set_free_data_start_ptr(PAGE_SIZE)
        # Page is marked dirty by _set methods

    def get_available_free_space(self) -> int:
        """Calculates the amount of contiguous free space available on the page."""
        num_records = self.get_num_records()
        end_of_slot_array = PAGE_HEADER_SIZE + (num_records * SLOT_ENTRY_SIZE)
        free_data_ptr = self.get_free_data_start_ptr()

        if free_data_ptr < end_of_slot_array:  # Should not happen in a consistent page
            return 0
        return free_data_ptr - end_of_slot_array

    def insert_record(self, record_data: bytes) -> int | None:
        """
        Inserts a new record into the page.
        Returns the slot_num if successful, None otherwise.
        Records are added by appending a new slot and writing data from the end of the page.
        Does not reuse deleted slots in this simple version.
        """
        record_len = len(record_data)
        if not record_len:
            print("Warning: Attempting to insert empty record data.")
            return None  # Or handle as a special case if allowed

        space_needed_for_data = record_len
        space_needed_for_new_slot_entry = SLOT_ENTRY_SIZE

        current_num_records = self.get_num_records()
        current_free_data_start = self.get_free_data_start_ptr()

        # Calculate where the new slot entry would end
        end_of_new_slot_array = PAGE_HEADER_SIZE + \
            ((current_num_records + 1) * SLOT_ENTRY_SIZE)

        # Check if there's enough space:
        # The end of the new slot array must be less than or equal to
        # the start of new data (current_free_data_start - space_needed_for_data)
        if end_of_new_slot_array > (current_free_data_start - space_needed_for_data):
            return None

        # if we reach here, there's enough space
        new_slot_num = current_num_records

        # Write record data from the end of the page (current free data start)
        new_record_offset_on_page = current_free_data_start - record_len
        self.page.data[new_record_offset_on_page: new_record_offset_on_page +
                       record_len] = record_data

        # Update slot information for the new record
        # The slot itself is located at _get_slot_offset_on_page(new_slot_num),
        # but we write directly as we know num_records will be incremented.
        self._set_slot_record_offset(new_slot_num, new_record_offset_on_page)
        self._set_slot_record_length(new_slot_num, record_len)

        self.page.mark_dirty()  # Ensure page is marked dirty
        return new_slot_num

    def get_record(self, slot_num: int) -> bytes | None:
        """
        Retrieves a record's data from the specified slot_num.
        Returns the record data as bytes, or None if slot_num is invalid or record is deleted.
        """
        num_records = self.get_num_records()
        if not (0 <= slot_num and slot_num < num_records):
            # print(f"Error: Slot number {slot_num} is out of range (0-{num_records-1}).")
            return None

        record_offset = self.get_slot_record_offset(slot_num)
        record_length = self.get_slot_record_length(slot_num)

        if record_length == 0 and record_length == 0:  # Another convention for deleted.
            # This might be redundant if length 0 is the primary check.
            return None

        # Basic sanity check for offset and length against page boundaries
        if record_offset < PAGE_HEADER_SIZE or (record_offset + record_length) > PAGE_SIZE:
            # This indicates corruption or an invalid slot
            # print(f"Error: Record in slot {slot_num} has invalid offset/length leading out of bounds.")
            return None

        return self.page.data[record_offset: record_offset + record_length]

    def delete_record(self, slot_num: int) -> bool:
        """
        Marks a record as deleted by setting its slot's record_length to 0.
        This simple version does not reclaim space or compact the page.
        Returns True if the slot_num was valid and marked, False otherwise.
        """
        num_records = self.get_num_records()
        if not (0 <= slot_num < num_records):
            return False

        # To "delete", we simply mark its length as 0.
        # The record_offset can remain, or also be set to 0 or an invalid marker.
        # Setting length to 0 is usually sufficient for get_record to identify it.
        # The actual data bytes remain on the page until overwritten or page is compacted.
        # For this version, we don't touch num_records as the slot still "exists" but is empty.
        # A more advanced version might try to reuse these slots.

        # Check if already "deleted" (idempotency)
        if self.get_slot_record_length(slot_num) == 0:
            return True  # Already marked as deleted

        self._set_slot_record_length(slot_num, 0)
        # Optionally, also set offset to an invalid marker if desired:
        # self._set_slot_record_offset(slot_num, 0) # Or some INVALID_OFFSET

        self.page.mark_dirty()
        return True

    # update_record is more complex if size changes.
    # For now, if an update is needed, it would be a delete() + insert(), resulting in a new RID potentially.
    # Or, only allow updates if the new data has the same or smaller length.
    # Let's defer a direct update_record method for now.

    def get_all_valid_rids_and_records(self) -> list[tuple[int, bytes]]:
        """
        Iterates over all slots and returns a list of (slot_num, record_data)
        for all valid (non-deleted) records.
        Useful for scanning all records on a page.
        """
        records = []
        # This is actually number of allocated slots
        num_total_slots = self.get_num_records()
        for slot_idx in range(num_total_slots):
            length = self.get_slot_record_length(slot_idx)
            if length > 0:  # Valid record
                record_data = self.get_record(slot_idx)
                if record_data is not None:  # Should not be None if length > 0
                    records.append((slot_idx, record_data))
        return records
