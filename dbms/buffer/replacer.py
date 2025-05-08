from abc import ABC, abstractmethod
import threading


class Replacer(ABC):
    """
    Abstract base class for a page replacer algorithm.
    Manages frame IDs that are candidates for replacement.
    """

    def __init__(self, num_frames: int):
        self._num_frames = num_frames
        self._lock = threading.Lock()

    @abstractmethod
    def victim(self) -> int | None:
        """
        Finds a frame to be replaced (victim).
        Returns:
            The frame ID of the victim, or None if no frame can be victimized
            (e.g., all are pinned or no unpinned frames available).
        """
        pass

    @abstractmethod
    def pin(self, frame_id: int) -> None:
        """
        Pins a frame, indicating it should not be considered for replacement.
        This typically removes the frame_id from the replacer's list of candidates.
        """
        pass

    @abstractmethod
    def unpin(self, frame_id: int) -> None:
        """
        Unpins a frame, indicating it can now be considered for replacement.
        This typically adds the frame_id to the replacer's list of candidates.
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Returns the number of frames currently available for replacement.
        """
        pass


class LRUReplacer(Replacer):
    """
    Least Recently Used (LRU) replacer.
    Maintains a list of frame_ids, with the least recently used at the head
    and most recently used at the tail.
    """

    def __init__(self, num_frames: int):
        super().__init__(num_frames)
        self._candidate_frames: list[int] = []  # stores frame_ids

    def victim(self) -> int | None:
        """
        Returns the least recently used frame_id (front of the list).
        Removes it from the candidate list.
        """
        with self._lock:
            if not self._candidate_frames:
                return None

            victim_frame_id = self._candidate_frames.pop(
                0)  # Evict from the front (LRU)
            return victim_frame_id

    def pin(self, frame_id: int) -> None:
        """
        If frame_id is in candidate_frames, it's removed because it's pinned.
        """
        with self._lock:
            if frame_id in self._candidate_frames:
                self._candidate_frames.remove(frame_id)

    def unpin(self, frame_id: int) -> None:
        """
        Adds frame_id to the end of candidate_frames (marking it as most recently unpinned/used).
        If it's already there (should not happen if logic is correct), remove first to avoid duplicates
        and ensure it moves to the MRU position.
        """
        with self._lock:
            if frame_id in self._candidate_frames:  # Should not happen if pin/unpin logic is correct
                self._candidate_frames.remove(frame_id)

            self._candidate_frames.append(frame_id)  # Add to the end (MRU)

    def size(self) -> int:
        """Returns the number of frames currently in the candidate list."""
        with self._lock:
            return len(self._candidate_frames)
