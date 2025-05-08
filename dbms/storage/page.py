from dbms.common.config import PAGE_SIZE, INVALID_PAGE_ID


class Page:
    """
    Represents a single page in memory, corresponding to a block of data on disk.
    """

    def __init__(self, page_id: int = INVALID_PAGE_ID):
        self._data: bytearray = bytearray(PAGE_SIZE)
        self._page_id: int = page_id
        self._pin_count: int = 0  # How many users are currently using this page
        # True if an in-memory copy is modified and not yet flushed to disk
        self._is_dirty: bool = False

    @property
    def data(self) -> bytearray:
        """Returns the bytearray data of the page."""
        return self._data

    @data.setter
    def data(self, new_data: bytes | bytearray) -> None:
        """Sets the page's data. Ensures the new data fits PAGE_SIZE."""
        if len(new_data) > PAGE_SIZE:
            raise ValueError(
                f"Data size {len(new_data)} exceeds PAGE_SIZE {PAGE_SIZE}")
        # If new_data is smaller, self._data will be a mix.
        # It's usually expected that new_data is PAGE_SIZE or handled by caller.
        self._data[:len(new_data)] = new_data
        # If new_data is smaller than PAGE_SIZE, the rest of self._data remains.
        # To clear it, one might do:
        # self._data = bytearray(PAGE_SIZE)
        # self._data[:len(new_data)] = new_data

    @property
    def page_id(self) -> int:
        """Get page ID

        Returns:
            int: page_id
        """
        return self._page_id

    @page_id.setter
    def page_id(self, pid: int) -> None:
        self._page_id = pid

    @property
    def pin_count(self) -> int:
        """Returns the pin count of the page."""
        return self._pin_count

    @property
    def is_dirty(self) -> bool:
        """Returns True if the page is dirty, False otherwise."""
        return self._is_dirty

    def increment_pin_count(self) -> None:
        """Increments the pin count."""
        self._pin_count += 1

    def decrement_pin_count(self) -> None:
        """
        Decrements the pin count.
        Raises ValueError if pin count is already 0.
        """
        if self._pin_count > 0:
            self._pin_count -= 1
        else:
            # This case should ideally not happen if managed correctly by BufferPoolManager
            raise ValueError(
                f"Attempted to decrement pin count for page {self._page_id} when it's already 0.")

    def mark_dirty(self) -> None:
        """Marks the page as dirty."""
        self._is_dirty = True

    def mark_clean(self) -> None:
        """Marks the page as clean."""
        self._is_dirty = False

    def get_data_view(self) -> memoryview:
        """Provides a memoryview of the page data for efficient access."""
        return memoryview(self._data)

    def reset_memory(self) -> None:
        """Resets the page's memory to zeros and its metadata."""
        self._data = bytearray(PAGE_SIZE)  # Re-initialize or fill with zeros
        # self._page_id = INVALID_PAGE_ID # Should be set by BufferPoolManager when reusing
        self._pin_count = 0
        self._is_dirty = False

    def __str__(self) -> str:
        return f"Page(id={self._page_id}, dirty={self._is_dirty}, pins={self._pin_count})"
