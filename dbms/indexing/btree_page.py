# --- Constants ---
import struct
from dbms.common.config import INVALID_PAGE_ID, PAGE_SIZE
from dbms.storage.page import Page


NODE_TYPE_LEAF = 0
NODE_TYPE_INTERNAL = 1

# --- Header Offsets
# Common Header
OFFSET_NODE_TYPE = 0  # Size: 1 byte
OFFSET_KEY_COUNT = 1  # Size: 2 bytes
OFFSET_PARENT_PAGE_ID = 3  # Size: 4 bytes
# Leaf-Specific Header (can extend common header space)
OFFSET_LEAF_PREV_PAGE_ID = 7  # Size: 4 bytes (Only if node_type is LEAF)
OFFSET_LEAF_NEXT_PAGE_ID = 11  # Size: 4 bytes (Only if node_type is LEAF)
# Start of actual key/pointer data (depends on final header size)
HEADER_SIZE_INTERNAL = 7  # Example: 1 (type) + 2 (key_count) + 4 (parent)
HEADER_SIZE_LEAF = (
    15  # Example: 1 (type) + 2 (key_count) + 4 (parent) + 4 (prev) + 4 (next)
)


class BTreePage:
    def __init__(self, page: Page, key_type_info, value_type_info):
        self.page_data = page.data
        self.key_type_info = key_type_info
        self.value_type_info = value_type_info

    def get_node_type(self) -> int:
        return struct.unpack_from(">B", self.page_data, OFFSET_NODE_TYPE)[0]

    def set_node_type(self, node_type: int):
        # Store a single byte for node type
        struct.pack_into(">B", self.page_data, OFFSET_NODE_TYPE, node_type)

    def get_key_count(self) -> int:
        # Read 2 bytes as unsigned short for key count
        return struct.unpack_from(">H", self.page_data, OFFSET_KEY_COUNT)[0]

    def set_key_count(self, count: int):
        # Store 2 bytes as unsigned short for key count
        struct.pack_into(">H", self.page_data, OFFSET_KEY_COUNT, count)

    def get_parent_page_id(self) -> int:
        # Read 4 bytes as signed int for parent page id
        return struct.unpack_from(">i", self.page_data, OFFSET_PARENT_PAGE_ID)[0]

    def set_parent_page_id(self, page_id: int):
        # Store 4 bytes as signed int for parent page id
        struct.pack_into(">i", self.page_data, OFFSET_PARENT_PAGE_ID, page_id)

    # --- Utility methods ---
    def is_leaf(self) -> bool:
        return self.get_node_type() == NODE_TYPE_LEAF

    def is_internal(self) -> bool:
        return self.get_node_type() == NODE_TYPE_INTERNAL

    def get_max_keys(
        self, key_size: int, value_or_pointer_size: int, header_size: int
    ) -> int:
        """Calculates max keys based on page size and item sizes."""
        page_size = len(self.page_data)  # Assuming page_data is full page size
        space_for_data = page_size - header_size
        if self.is_leaf():
            # Each entry: key + value (RID)
            if (key_size + value_or_pointer_size) == 0:
                return 0
            return space_for_data // (key_size + value_or_pointer_size)
        else:
            # Internal: N keys, N+1 pointers
            # N * key_size + (N+1) * pointer_size <= space_for_data
            # N <= (space_for_data - pointer_size) // (key_size + pointer_size)
            if (key_size + value_or_pointer_size) == 0:
                return 0
            return (space_for_data - value_or_pointer_size) // (
                key_size + value_or_pointer_size
            )


# --- BTreeLeafPage Class ---
class BTreeLeafPage(BTreePage):
    def __init__(self, page_obj, key_type_info, rid_type_info):
        super().__init__(page_obj, key_type_info, rid_type_info)
        self.rid_type_info = rid_type_info

    def init_page(
        self,
        parent_page_id=INVALID_PAGE_ID,
        prev_page_id=INVALID_PAGE_ID,
        next_page_id=INVALID_PAGE_ID,
    ):
        self.set_node_type(NODE_TYPE_LEAF)
        self.set_key_count(0)
        self.set_parent_page_id(parent_page_id)
        self.set_prev_leaf_page_id(prev_page_id)
        self.set_next_leaf_page_id(next_page_id)

    # --- Leaf-Specific Header Accessors ---
    def get_prev_leaf_page_id(self) -> int:
        return struct.unpack_from(">i", self.page_data, OFFSET_LEAF_PREV_PAGE_ID)[0]

    def set_prev_leaf_page_id(self, page_id: int):
        struct.pack_into(">i", self.page_data, OFFSET_LEAF_PREV_PAGE_ID, page_id)

    def get_next_leaf_page_id(self) -> int:
        return struct.unpack_from(">i", self.page_data, OFFSET_LEAF_NEXT_PAGE_ID)[0]

    def set_next_leaf_page_id(self, page_id: int):
        struct.pack_into(">i", self.page_data, OFFSET_LEAF_NEXT_PAGE_ID, page_id)

    # --- Key-RID Pair Management ---
    def get_key_at(self, index: int):
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        offset = HEADER_SIZE_LEAF + index * (key_size + rid_size)
        return self.key_type_info.deserialize(self.page_data, offset)

    def set_key_at(self, index: int, key):
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        offset = HEADER_SIZE_LEAF + index * (key_size + rid_size)
        self.key_type_info.serialize(key, self.page_data, offset)

    def get_rid_at(self, index: int):
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        offset = HEADER_SIZE_LEAF + index * (key_size + rid_size) + key_size
        return self.rid_type_info.deserialize(self.page_data, offset)

    def set_rid_at(self, index: int, rid):
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        offset = HEADER_SIZE_LEAF + index * (key_size + rid_size) + key_size
        self.rid_type_info.serialize(rid, self.page_data, offset)

    def find_key_index(self, key):
        # Binary search for sorted keys
        left = 0
        right = self.get_key_count() - 1
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.get_key_at(mid)
            if mid_key == key:
                return mid, True
            elif mid_key < key:
                left = mid + 1
            else:
                right = mid - 1
        return left, False

    def insert_key_rid_pair(self, key, rid):
        key_count = self.get_key_count()
        key_size = self.key_type_info.size
        rid_size = self.rid_type_info.size
        max_keys = self.get_max_keys(key_size, rid_size, HEADER_SIZE_LEAF)
        if key_count >= max_keys:
            return False
        insert_index, found = self.find_key_index(key)
        if found:
            return False  # No duplicates
        # Shift keys and rids to the right
        for i in range(key_count, insert_index, -1):
            self.set_key_at(i, self.get_key_at(i - 1))
            self.set_rid_at(i, self.get_rid_at(i - 1))
        self.set_key_at(insert_index, key)
        self.set_rid_at(insert_index, rid)
        self.set_key_count(key_count + 1)
        return True

    def remove_key(self, key):
        key_count = self.get_key_count()
        index, found = self.find_key_index(key)
        if not found:
            return False
        # Shift keys and rids to the left
        for i in range(index, key_count - 1):
            self.set_key_at(i, self.get_key_at(i + 1))
            self.set_rid_at(i, self.get_rid_at(i + 1))
        self.set_key_count(key_count - 1)
        return True


# --- BTreeInternalPage Class ---
class BTreeInternalPage(BTreePage):
    def __init__(self, page_obj, key_type_info, page_id_type_info):
        super().__init__(page_obj, key_type_info, page_id_type_info)
        self.page_id_type_info = page_id_type_info

    def init_page(self, parent_page_id=INVALID_PAGE_ID):
        self.set_node_type(NODE_TYPE_INTERNAL)
        self.set_key_count(0)
        self.set_parent_page_id(parent_page_id)

    # --- Key-Pointer Pair Management (P0, K1, P1, K2, P2 ... Kn, Pn) ---
    def get_key_at(self, index: int):
        key_size = self.key_type_info.size
        pointer_size = self.page_id_type_info.size
        offset = HEADER_SIZE_INTERNAL + pointer_size + index * (key_size + pointer_size)
        return self.key_type_info.deserialize(self.page_data, offset)

    def set_key_at(self, index: int, key):
        key_size = self.key_type_info.size
        pointer_size = self.page_id_type_info.size
        offset = HEADER_SIZE_INTERNAL + pointer_size + index * (key_size + pointer_size)
        self.key_type_info.serialize(key, self.page_data, offset)

    def get_pointer_at(self, index: int):
        key_size = self.key_type_info.size
        pointer_size = self.page_id_type_info.size
        offset = HEADER_SIZE_INTERNAL + index * (key_size + pointer_size)
        return self.page_id_type_info.deserialize(self.page_data, offset)

    def set_pointer_at(self, index: int, page_id):
        key_size = self.key_type_info.size
        pointer_size = self.page_id_type_info.size
        offset = HEADER_SIZE_INTERNAL + index * (key_size + pointer_size)
        self.page_id_type_info.serialize(page_id, self.page_data, offset)

    def lookup_child_page_id(self, key):
        key_count = self.get_key_count()
        if key_count == 0:
            return self.get_pointer_at(0)
        # Binary search
        left = 0
        right = key_count - 1
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.get_key_at(mid)
            if key < mid_key:
                right = mid - 1
            else:
                left = mid + 1
        # left is the pointer index to follow
        return self.get_pointer_at(left)

    def insert_key_pointer_pair(self, key, right_child_page_id):
        key_count = self.get_key_count()
        key_size = self.key_type_info.size
        pointer_size = self.page_id_type_info.size
        max_keys = self.get_max_keys(key_size, pointer_size, HEADER_SIZE_INTERNAL)
        if key_count >= max_keys:
            return False
        # Find insert position
        left = 0
        right = key_count - 1
        while left <= right:
            mid = (left + right) // 2
            mid_key = self.get_key_at(mid)
            if key < mid_key:
                right = mid - 1
            else:
                left = mid + 1
        insert_index = left
        # Shift keys and pointers to the right
        for i in range(key_count, insert_index, -1):
            self.set_key_at(i, self.get_key_at(i - 1))
        for i in range(key_count + 1, insert_index + 1, -1):
            self.set_pointer_at(i, self.get_pointer_at(i - 1))
        self.set_key_at(insert_index, key)
        self.set_pointer_at(insert_index + 1, right_child_page_id)
        self.set_key_count(key_count + 1)
        return True
