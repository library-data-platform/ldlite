import logging
import re
from typing import TYPE_CHECKING, Any

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
