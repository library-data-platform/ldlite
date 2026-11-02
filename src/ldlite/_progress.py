import logging
import re
from contextlib import AbstractContextManager, ExitStack
from typing import TYPE_CHECKING, Any, Self, cast

from tqdm.contrib.logging import logging_redirect_tqdm

if TYPE_CHECKING:
    from typing import NoReturn

    from tqdm import tqdm

    Progress = tqdm[NoReturn]
else:
    from tqdm import tqdm

    Progress = tqdm

logger = logging.getLogger("ldlite")


class LoggingTqdm(Progress):
    def __init__(self, *args: Any, disable: bool = False, **kwargs: Any) -> None:  # noqa: ANN401
        self._lcm = ExitStack()
        self._logonly = disable
        if self._logonly:
            kwargs["bar_format"] = re.sub(
                r"\{bar\}[\|\s]*",
                "",
                kwargs.get("bar_format", "{l_bar}{bar}{r_bar}"),
            )

        kwargs["disable"] = False
        super().__init__(*args, **kwargs)

    def display(self, msg: str | None = None, pos: int | None = None) -> None:
        if self._logonly:
            logger.info(self.format_meter(**self.format_dict))
            return

        super().display(msg, pos)

    def __enter__(self) -> Self:
        if not self._logonly:
            self._lcm.enter_context(
                cast("AbstractContextManager[None]", logging_redirect_tqdm()),
            )

        return super().__enter__()

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self._lcm.close()
        super().__exit__(exc_type, exc_value, traceback)
