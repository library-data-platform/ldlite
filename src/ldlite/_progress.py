from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing import NoReturn

    from tqdm import tqdm

    Progress = tqdm[NoReturn]
else:
    from tqdm import tqdm

    Progress = tqdm


class LoggingTqdm(Progress):
    def __init__(self, *args: Any, disable: bool = False, **kwargs: Any) -> None:  # noqa: ANN401
        self._logonly = disable

        kwargs["disable"] = False
        super().__init__(*args, **kwargs)

    def display(self, msg: str | None = None, pos: int | None = None) -> None:
        if self._logonly:
            return

        super().display(msg, pos)
