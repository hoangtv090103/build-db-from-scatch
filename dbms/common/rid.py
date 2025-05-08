from typing import NamedTuple
from dbms.commom.config import INVALID_PAGE_ID


class RID(NamedTuple):
    """
    Record Identifier. Uniquely identifies a record within the database
    Consists of a page_id and a slot_num (or offset within the page)
    """
    page_id: int = INVALID_PAGE_ID
    slot_num: int = -1  # -1 indicates an invalid slot number

    def __str__(self) -> str:
        return f"RID(page_id={self.page_id}, slot_num={self.slot_num})"

    def is_valid(self) -> bool:
        """Checks if the RID is valid."""
        return self.page_id != INVALID_PAGE_ID and self.slot_num != -1


INVALID_RID = RID()  # A globally accessible invalid RID instance
