"""Progress bar utilities using rich."""

from rich.console import Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.text import Text

from .output import console


def _fmt_size(size_bytes: int | float) -> str:
    """
    Format bytes as a fixed-width 9-char string.
    """
    for unit in ["B ", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:6.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:6.1f} PB"


class SpeedColumn(ProgressColumn):
    """
    Transfer speed as fixed-width string.
    """

    def render(self, task: Task) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("           ", style="dim")
        return Text(f"{_fmt_size(speed)}/s", style="dim")


class SizeColumn(ProgressColumn):
    """
    Completed/total size as fixed-width string.
    """

    def render(self, task: Task) -> Text:
        completed = _fmt_size(int(task.completed))
        total = _fmt_size(int(task.total) if task.total else 0)
        return Text(f"{completed}/{total}", style="dim")


class FilenameColumn(ProgressColumn):
    """
    Filename truncated/padded to fixed width.
    """

    WIDTH = 48

    def render(self, task: Task) -> Text:
        name = task.fields.get("filename", "")
        if len(name) > self.WIDTH:
            name = "..." + name[-(self.WIDTH - 3) :]
        return Text(f"{name:>{self.WIDTH}}", style="")


class TotalLabelColumn(ProgressColumn):
    """
    Label column sized to match filename column above.
    """

    WIDTH = FilenameColumn.WIDTH

    def render(self, task: Task) -> Text:
        label = task.description
        return Text(f"{label:>{self.WIDTH}}", style="bold")


def _create_file_progress() -> Progress:
    """
    Create a progress bar for individual files.
    """
    return Progress(
        FilenameColumn(),
        BarColumn(bar_width=20, complete_style="green", finished_style="green"),
        TextColumn("{task.percentage:>4.0f}%"),
        SizeColumn(),
        SpeedColumn(),
        console=console,
    )


def _create_overall_progress() -> Progress:
    """
    Create a progress bar for overall operation.
    """
    return Progress(
        TotalLabelColumn(),
        BarColumn(bar_width=20, complete_style="green", finished_style="green"),
        TextColumn("{task.percentage:>4.0f}%"),
        SizeColumn(),
        SpeedColumn(),
        TimeElapsedColumn(),
        console=console,
    )


class TransferProgress:
    """
    Manages progress display for file transfers.
    """

    def __init__(self, operation: str = "Uploading", max_visible_files: int = 4):
        self.operation = operation
        self._max_visible = max_visible_files
        self.file_progress = _create_file_progress()
        self.overall_progress = _create_overall_progress()
        self._live: Live | None = None
        self._file_tasks: dict[str, TaskID] = {}
        self._overall_task: TaskID | None = None
        self._total_bytes: int = 0
        self._completed_bytes: int = 0
        self._total_files: int = 0
        self._completed_files: int = 0

    def _make_display(self) -> Group:
        """
        Create the display group with fixed height.
        """
        active_count = sum(1 for t in self.file_progress.tasks if t.visible)
        pad_lines = max(0, self._max_visible - active_count)

        parts: list[Text | Progress] = []
        parts.append(self.overall_progress)
        parts.append(self.file_progress)
        for _ in range(pad_lines):
            parts.append(Text(""))

        return Group(*parts)

    def __enter__(self) -> "TransferProgress":
        """
        Enter context and start live display.
        """
        if not console.quiet:
            self._live = Live(self._make_display(), console=console, refresh_per_second=4)
            self._live.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit context and stop live display.
        """
        if self._live:
            try:
                self._live.update(Text(""))
            except Exception:
                pass
            self._live.__exit__(exc_type, exc_val, exc_tb)

    @property
    def elapsed_seconds(self) -> float:
        """
        Get elapsed time from the overall progress task.
        """
        if self._overall_task is not None:
            task = self.overall_progress.tasks[self._overall_task]
            return task.elapsed or 0.0
        return 0.0

    def set_total_size(self, total_bytes: int) -> None:
        """
        Set the total size for overall progress.
        """
        self._total_bytes = total_bytes
        self._overall_task = self.overall_progress.add_task(
            f"[{self._completed_files}/{self._total_files}]",
            total=total_bytes,
        )

    def set_total_files(self, count: int) -> None:
        """
        Set total file count for the header.
        """
        self._total_files = count
        self._update_total_label()

    def add_file(self, filename: str, size: int) -> TaskID:
        """
        Add an active file to the display.
        """
        task_id = self.file_progress.add_task(
            filename,
            filename=filename,
            total=size,
        )
        self._file_tasks[filename] = task_id
        self._update_display()
        return task_id

    def update_file(self, filename: str, completed: int) -> None:
        """
        Update progress for a specific file.
        """
        if filename in self._file_tasks:
            task_id = self._file_tasks[filename]
            task = self.file_progress.tasks[task_id]
            delta = completed - task.completed
            self.file_progress.update(task_id, completed=completed)

            if self._overall_task is not None and delta > 0:
                self._completed_bytes += int(delta)
                self.overall_progress.update(self._overall_task, completed=self._completed_bytes)

    def advance_file(self, filename: str, advance: int) -> None:
        """
        Advance progress for a specific file.
        """
        if filename in self._file_tasks:
            task_id = self._file_tasks[filename]
            self.file_progress.advance(task_id, advance)

            if self._overall_task is not None:
                self._completed_bytes += int(advance)
                self.overall_progress.update(self._overall_task, completed=self._completed_bytes)

    def complete_file(self, filename: str) -> None:
        """
        Mark a file as complete and hide it from the active display.
        """
        if filename in self._file_tasks:
            task_id = self._file_tasks[filename]
            task = self.file_progress.tasks[task_id]
            remaining = (task.total or 0) - (task.completed or 0)

            if self._overall_task is not None and remaining > 0:
                self._completed_bytes += int(remaining)
                self.overall_progress.update(self._overall_task, completed=self._completed_bytes)

            self.file_progress.update(task_id, visible=False)
            del self._file_tasks[filename]
            self._completed_files += 1
            self._update_total_label()
            self._update_display()

    def remove_file(self, filename: str) -> None:
        """
        Remove a file from tracking.
        """
        if filename in self._file_tasks:
            task_id = self._file_tasks[filename]
            self.file_progress.update(task_id, visible=False)
            del self._file_tasks[filename]

    def _update_total_label(self) -> None:
        """
        Update the total progress label with current file count.
        """
        if self._overall_task is not None:
            self.overall_progress.update(
                self._overall_task,
                description=f"[{self._completed_files}/{self._total_files}]",
            )

    def _update_display(self) -> None:
        """
        Force display refresh.
        """
        if self._live:
            self._live.update(self._make_display())
